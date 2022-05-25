package es.unizar.iaaa.frontier.service

import crawlercommons.urlfrontier.URLFrontierGrpcKt
import es.unizar.iaaa.frontier.service.Fixtures.CRAWL_ID_A
import es.unizar.iaaa.frontier.service.Fixtures.EMPTY_KEY
import es.unizar.iaaa.frontier.service.utils.GrpcCleanupExtension
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.extension.RegisterExtension
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class AbstractFrontierServiceTest {

    @RegisterExtension
    @JvmField
    val grpcExtension: GrpcCleanupExtension = GrpcCleanupExtension()

    private lateinit var stub: URLFrontierGrpcKt.URLFrontierCoroutineStub
    private lateinit var service: AbstractFrontierService<String>

    @BeforeTest
    fun setupServer() {
        service = FixtureAbstractFrontierService()
        stub = with(grpcExtension) {
            val serverName = serverName()
            registerServer(serverName, service)
            URLFrontierGrpcKt.URLFrontierCoroutineStub(registerChannel(serverName))
        }
    }

    @Test
    fun `The frontier is active by default`() {
        runBlocking {
            val reply = stub.getActive()
            assertTrue(reply)
        }
    }

    @Test
    fun `I can deactivate the frontier`() {
        runBlocking {
            stub.setActive(false)
            val reply = stub.getActive()
            assertFalse(reply)
        }
    }

    @Test
    fun `I can set a default delay for queues`() {
        runBlocking {
            assertEquals(1, service.defaultDelayForQueues)
            stub.setDelay(EMPTY_KEY, 100, CRAWL_ID_A)
            assertEquals(100, service.defaultDelayForQueues)
        }
    }
}
