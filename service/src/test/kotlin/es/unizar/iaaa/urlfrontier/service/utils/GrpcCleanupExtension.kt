package es.unizar.iaaa.urlfrontier.service.utils

import io.grpc.BindableService
import io.grpc.Channel
import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.ExtensionContext
import java.util.concurrent.TimeUnit

class GrpcCleanupExtension : AfterEachCallback {
    private val resources: MutableList<Resource> = mutableListOf()
    private var timeoutNanos = TimeUnit.SECONDS.toNanos(10L)
    private var stopwatch: Stopwatch = Stopwatch()
    private var firstException: Throwable? = null

    fun serverName(): String = InProcessServerBuilder.generateName()
    fun registerServer(serverName: String, service: BindableService): Server = register(
        InProcessServerBuilder.forName(serverName).directExecutor().addService(service).build().start()
    )

    fun registerChannel(serverName: String): Channel =
        register(InProcessChannelBuilder.forName(serverName).directExecutor().build())

    /**
     * Sets a positive total time limit for the automatic resource cleanup. If any of the resources
     * registered to the rule fails to be released in time, the test will fail.
     *
     *
     * Note that the resource cleanup duration may or may not be counted as part of the JUnit
     * [Timeout][org.junit.rules.Timeout] rule's test duration, depending on which rule is
     * applied first.
     *
     * @return this
     */
    fun setTimeout(timeout: Long, timeUnit: TimeUnit): GrpcCleanupExtension {
        require(timeout > 0) { "timeout should be positive" }
        timeoutNanos = timeUnit.toNanos(timeout)
        return this
    }

    /**
     * Registers the given channel to the rule. Once registered, the channel will be automatically
     * shutdown at the end of the test.
     *
     *
     * This method need be properly synchronized if used in multiple threads. This method must
     * not be used during the test teardown.
     *
     * @return the input channel
     */
    fun <T : ManagedChannel> register(channel: T): T {
        register(ManagedChannelResource(channel))
        return channel
    }

    /**
     * Registers the given server to the rule. Once registered, the server will be automatically
     * shutdown at the end of the test.
     *
     *
     * This method need be properly synchronized if used in multiple threads. This method must
     * not be used during the test teardown.
     *
     * @return the input server
     */
    fun <T : Server> register(server: T): T {
        register(ServerResource(server))
        return server
    }

    fun register(resource: Resource) {
        resources.add(resource)
    }

    /**
     * Releases all the registered resources.
     */
    private fun teardown() {
        stopwatch.reset()
        stopwatch.start()
        if (firstException == null) {
            for (i in resources.size - 1 downTo 0) {
                resources[i].cleanUp()
            }
        }
        for (i in resources.size - 1 downTo 0) {
            if (firstException != null) {
                resources[i].forceCleanUp()
                continue
            }
            try {
                val released = resources[i].awaitReleased(
                    timeoutNanos - stopwatch.elapsed(), TimeUnit.NANOSECONDS
                )
                if (!released) {
                    firstException = AssertionError(
                        "Resource " + resources[i] + " can not be released in time at the end of test"
                    )
                }
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                firstException = e
            }
            if (firstException != null) {
                resources[i].forceCleanUp()
            }
        }
        resources.clear()
    }

    interface Resource {
        fun cleanUp()

        /**
         * Error already happened, try the best to clean up. Never throws.
         */
        fun forceCleanUp()

        /**
         * Returns true if the resource is released in time.
         */
        @Throws(InterruptedException::class)
        fun awaitReleased(duration: Long, timeUnit: TimeUnit?): Boolean
    }

    private class ManagedChannelResource(val channel: ManagedChannel) : Resource {
        override fun cleanUp() {
            channel.shutdown()
        }

        override fun forceCleanUp() {
            channel.shutdownNow()
        }

        @Throws(InterruptedException::class)
        override fun awaitReleased(duration: Long, timeUnit: TimeUnit?): Boolean {
            return channel.awaitTermination(duration, timeUnit)
        }

        override fun toString(): String {
            return channel.toString()
        }
    }

    private class ServerResource(val server: Server) : Resource {
        override fun cleanUp() {
            server.shutdown()
        }

        override fun forceCleanUp() {
            server.shutdownNow()
        }

        @Throws(InterruptedException::class)
        override fun awaitReleased(duration: Long, timeUnit: TimeUnit?): Boolean {
            return server.awaitTermination(duration, timeUnit)
        }

        override fun toString(): String {
            return server.toString()
        }
    }

    override fun afterEach(context: ExtensionContext) {
        teardown()
    }
}

class Stopwatch {
    private var startNanos: Long = 0
    fun reset() {
        startNanos = 0
    }

    fun start() {
        startNanos = System.nanoTime()
    }

    fun elapsed(): Long = System.nanoTime() - startNanos
}
