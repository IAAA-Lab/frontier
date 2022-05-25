package es.unizar.iaaa.frontier.service

import crawlercommons.urlfrontier.Urlfrontier
import es.unizar.iaaa.frontier.service.memory.InternalURL
import es.unizar.iaaa.frontier.service.memory.MemoryFrontierService
import io.grpc.stub.StreamObserver
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

internal class URLFrontierServerUnitTest {

    private lateinit var server: URLFrontierServer

    @BeforeTest
    fun serverSetup() {
        server = URLFrontierServer()
    }

    @Test
    fun `build default service`() {
        server.configure()
        assertTrue(server.buildService() is MemoryFrontierService)
    }

    @Test
    fun `build service fails with a wrong implementation class`() {
        server.implementation = DummyClass1::class.qualifiedName
        server.configure()
        val message = assertFailsWith<Exception> {
            server.buildService()
        }.message
        assertEquals(
            "Implementation class ${DummyClass1::class.qualifiedName} does not extend AbstractFrontierService",
            message
        )
    }

    @Test
    fun `build service fails with an implementation class with a wrong parameter`() {
        server.implementation = DummyClass2::class.qualifiedName
        server.configure()
        val message = assertFailsWith<Exception> {
            server.buildService()
        }.message
        assertEquals(
            "Implementation class ${DummyClass2::class.qualifiedName} constructor is not (FrontierProperties)",
            message
        )
    }

    @Test
    fun `build service fails with an implementation class with too many parameters`() {
        server.implementation = DummyClass3::class.qualifiedName
        server.configure()
        val message = assertFailsWith<Exception> {
            server.buildService()
        }.message
        assertEquals(
            "Implementation class ${DummyClass3::class.qualifiedName} constructor has more than one parameter",
            message
        )
    }

    @Test
    fun `build a valid service`() {
        server.implementation = DummyClass4::class.qualifiedName
        server.configure()
        assertTrue(server.buildService() is DummyClass4)
    }

    @Test
    fun `positional parameters cannot set the service`() {
        server.positional = listOf("implementation=${DummyClass4::class.qualifiedName}")
        server.configure()
        assertTrue(server.buildService() is MemoryFrontierService)
    }
}

internal class DummyClass1
internal class DummyClass2(val a: String)
internal class DummyClass3(val a: String, val b: String)
internal class DummyClass4(val a: FrontierProperties) : AbstractFrontierService<InternalURL>() {
    override fun sendURLsForQueue(
        sendURLsCommand: SendURLsCommand<InternalURL>,
        responseObserver: StreamObserver<Urlfrontier.URLInfo>
    ): Int {
        TODO()
    }
}
