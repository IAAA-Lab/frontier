package es.unizar.iaaa.urlfrontier.service

import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.MapPropertySource
import com.sksamuel.hoplite.PropertySource
import com.sksamuel.hoplite.addResourceOrFileSource
import crawlercommons.urlfrontier.URLFrontierGrpc.URLFrontierImplBase
import es.unizar.iaaa.urlfrontier.service.memory.MemoryFrontierService
import es.unizar.iaaa.urlfrontier.service.runtime.runtime
import es.unizar.iaaa.urlfrontier.service.runtime.shutdownHook
import io.grpc.Server
import io.grpc.ServerBuilder
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports
import mu.KotlinLogging
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters
import java.util.concurrent.Callable
import kotlin.reflect.full.primaryConstructor
import kotlin.system.exitProcess

private val logger = KotlinLogging.logger {}

@Command(name = "URL Frontier Server", mixinStandardHelpOptions = true, version = ["1.2"])
class URLFrontierServer : Callable<Int> {

    private val options = FrontierOptions()

    @Option(
        names = ["-p", "--port"],
        defaultValue = "7071",
        paramLabel = "NUM",
        description = ["URL Frontier port (default to 7071)"]
    )
    var port = 0

    @Option(
        names = ["-c", "--config"],
        required = false,
        paramLabel = "STRING",
        description = ["key value configuration file"]
    )
    var config: String? = null

    @Option(
        names = ["-s", "--prometheus_server"],
        paramLabel = "NUM",
        description = ["Port number to use for Prometheus server"]
    )
    var prometheusPort = options.prometheusPort

    @Option(
        names = ["-i", "--implementation"],
        paramLabel = "STRING",
        description = ["Frontier implementation class"]
    )
    var implementation: String? = null

    @Parameters
    var positional: List<String>? = null

    private lateinit var properties: FrontierProperties
    private lateinit var configuration: Configuration

    fun configure() {
        val updatedOptions = options.copy(
            port = port,
            prometheusPort = prometheusPort,
            implementation = implementation
        )
        properties = FrontierProperties(updatedOptions, config, positional)
        configuration = properties.config(options.toPropertySource())
    }

    var runtimeConfig: RuntimeConfig? = null

    val isRunning: Boolean get() = runtimeConfig != null

    private fun doShutdown(server: Server, prometheus: HTTPServer?) {
        runCatching {
            // according to https://fedor.medium.com/shutting-down-grpc-services-gracefully-961a95b08f8
            // some extra work may be required for streams
            logger.info { "Shutting down URLFrontierServer on port ${server.port}" }
            server.shutdown()
            server.awaitTermination()
            prometheus?.let {
                logger.info { "Shutting down Prometheus server on port ${it.port}" }
                it.close()
            }
            Unit
        }.onFailure {
            logger.error(it) { "Error when trying to shutdown a lifecycle component: ${javaClass.name}" }
        }
    }

    fun shutdown(): Boolean = runtimeConfig?.let {
        runCatching {
            runtime.removeShutdownHook(it.shutdownHook)
            doShutdown(it.server, it.prometheus)
            runtimeConfig = null
            true
        }.onFailure {
            logger.error(it) { "Error when trying to shutdown a lifecycle component: ${javaClass.name}" }
        }.getOrThrow()
    } ?: false

    override fun call(): Int {
        if (runtimeConfig != null) {
            return -1
        }
        configure()
        runCatching {
            with(configuration.server) {
                val prometheus: HTTPServer? = if (prometheusPort > 0) {
                    DefaultExports.initialize()
                    logger.info { "Starting Prometheus server on port $prometheusPort" }
                    HTTPServer.Builder().withPort(prometheusPort).build()
                } else null
                val service = buildService()
                val server = ServerBuilder.forPort(port).addService(service).build().start()
                logger.info { "Started URLFrontierServer [${service.javaClass.simpleName}] on port $port" }
                Pair(server, prometheus)
            }
        }.onSuccess { (server, prometheus) ->
            runtimeConfig = RuntimeConfig(
                server = server,
                prometheus = prometheus,
                shutdownHook = runtime.shutdownHook {
                    doShutdown(server, prometheus)
                }
            )
            server.awaitTermination()
        }.onFailure {
            logger.error(it) { it.message }
            return -1
        }
        return 0
    }

    /**
     * Build a frontier service from the configuration
     */
    fun buildService(): URLFrontierImplBase {
        val implementationClass = Class.forName(configuration.server.implementation).kotlin
        val constructor = implementationClass.primaryConstructor
            ?: throw NoSuchMethodException("Implementation class ${implementationClass.qualifiedName} does not have a primary constructor")
        val obj = when (constructor.parameters.size) {
            0 -> constructor.call()
            1 -> runCatching { constructor.call(properties) }
                .onFailure {
                    throw NoSuchMethodException("Implementation class ${implementationClass.qualifiedName} constructor is not (FrontierProperties)")
                }.getOrThrow()
            else -> throw NoSuchMethodException("Implementation class ${implementationClass.qualifiedName} constructor has more than one parameter")
        }
        return obj as? AbstractFrontierService<*>
            ?: throw Exception("Implementation class ${implementationClass.qualifiedName} does not extend AbstractFrontierService")
    }

    data class Configuration(
        val server: ServerConfiguration
    )

    data class ServerConfiguration(
        val port: Int,
        val prometheusPort: Int,
        val implementation: String
    )

    data class RuntimeConfig(
        val shutdownHook: Thread,
        val server: Server,
        val prometheus: HTTPServer?
    )
}

fun main(args: Array<String>) {
    val cli = CommandLine(URLFrontierServer())
    val exitCode = cli.execute(*args)
    exitProcess(exitCode)
}

internal data class FrontierOptions(
    val port: Int = 0,
    val prometheusPort: Int = 0,
    val implementation: String? = MemoryFrontierService::class.qualifiedName
) {
    fun toPropertySource(): PropertySource {
        val map = listOf(
            PORT to port,
            PROMETHEUS_PORT to prometheusPort,
            IMPLEMENTATION to implementation
        ).filter { (_, v) -> v != null }.toMap()
        return MapPropertySource(map)
    }

    companion object {
        const val PORT = "server.port"
        const val PROMETHEUS_PORT = "server.prometheusPort"
        const val IMPLEMENTATION = "server.implementation"
    }
}

internal class FrontierProperties(
    private val options: FrontierOptions,
    private val config: String? = null,
    private val positional: List<String>? = null
) {

    inline fun <reified T : Any> config(defaults: PropertySource = empty()): T = with(ConfigLoaderBuilder.default()) {
        addPropertySource(optionsAsSource())
        addPropertySource(positionalAsSource())
        addConfig()
        addPropertySource(defaults)
        build()
    }.loadConfigOrThrow()

    fun empty(): PropertySource {
        return MapPropertySource(emptyMap())
    }

    fun ConfigLoaderBuilder.addConfig() {
        config?.let { addResourceOrFileSource(it) }
    }

    fun optionsAsSource(): PropertySource {
        return options.toPropertySource()
    }

    fun positionalAsSource(): PropertySource {
        val map = positional?.mapNotNull { parse(it) }?.toMap() ?: emptyMap()
        return MapPropertySource(map)
    }

    private fun parse(line: String): Pair<String, Any?>? {
        if (line.isEmpty()) return null
        val trimmedLine = line.trim { it <= ' ' }
        if (trimmedLine.isEmpty()) return null

        val pos = trimmedLine.indexOf('=')

        if (pos == -1) {
            return trimmedLine to null
        }
        val key = trimmedLine.substring(0, pos).trim { it <= ' ' }
        val value = trimmedLine.substring(pos + 1).trim { it <= ' ' }
        return key to value
    }
}
