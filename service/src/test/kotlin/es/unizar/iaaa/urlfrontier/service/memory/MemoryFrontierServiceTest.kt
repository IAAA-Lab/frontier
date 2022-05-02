package es.unizar.iaaa.urlfrontier.service.memory

import ch.qos.logback.classic.Level
import crawlercommons.urlfrontier.URLFrontierGrpcKt
import es.unizar.iaaa.urlfrontier.service.Fixtures.ALL_QUEUES_CRAWL_ID_A
import es.unizar.iaaa.urlfrontier.service.Fixtures.ALL_QUEUES_CRAWL_ID_A_START_1
import es.unizar.iaaa.urlfrontier.service.Fixtures.ALL_QUEUES_CRAWL_ID_B
import es.unizar.iaaa.urlfrontier.service.Fixtures.CRAWL_ID_A
import es.unizar.iaaa.urlfrontier.service.Fixtures.CRAWL_ID_B
import es.unizar.iaaa.urlfrontier.service.Fixtures.EXAMPLE_KEY_A
import es.unizar.iaaa.urlfrontier.service.Fixtures.EXAMPLE_KEY_B
import es.unizar.iaaa.urlfrontier.service.Fixtures.EXAMPLE_URL_A
import es.unizar.iaaa.urlfrontier.service.Fixtures.EXAMPLE_URL_C
import es.unizar.iaaa.urlfrontier.service.Fixtures.EXAMPLE_URL_D
import es.unizar.iaaa.urlfrontier.service.Fixtures.ONE_QUEUE_CRAWL_ID_A
import es.unizar.iaaa.urlfrontier.service.Fixtures.URL_DISCOVERED_A_IN_KEY_A_CRAWL_A
import es.unizar.iaaa.urlfrontier.service.Fixtures.URL_DISCOVERED_B_IN_KEY_A_CRAWL_A
import es.unizar.iaaa.urlfrontier.service.Fixtures.URL_DISCOVERED_B_IN_KEY_B_CRAWL_A
import es.unizar.iaaa.urlfrontier.service.Fixtures.URL_DISCOVERED_C_IN_KEY_B_CRAWL_A
import es.unizar.iaaa.urlfrontier.service.Fixtures.URL_DISCOVERED_C_IN_KEY_B_CRAWL_B
import es.unizar.iaaa.urlfrontier.service.Fixtures.URL_DISCOVERED_D_IN_KEY_B_CRAWL_A
import es.unizar.iaaa.urlfrontier.service.Fixtures.URL_DISCOVERED_D_IN_KEY_B_CRAWL_B
import es.unizar.iaaa.urlfrontier.service.Fixtures.URL_DISCOVERED_WITHOUT_URL_AND_KEY
import es.unizar.iaaa.urlfrontier.service.Fixtures.URL_KNOWN_NON_REFETCHABLE
import es.unizar.iaaa.urlfrontier.service.Fixtures.URL_KNOWN_REFETCHABLE
import es.unizar.iaaa.urlfrontier.service.blockQueueParams
import es.unizar.iaaa.urlfrontier.service.empty
import es.unizar.iaaa.urlfrontier.service.getParams
import es.unizar.iaaa.urlfrontier.service.putUrls
import es.unizar.iaaa.urlfrontier.service.queueKey
import es.unizar.iaaa.urlfrontier.service.queueWithinCrawlParams
import es.unizar.iaaa.urlfrontier.service.setActive
import es.unizar.iaaa.urlfrontier.service.setDelay
import es.unizar.iaaa.urlfrontier.service.string
import es.unizar.iaaa.urlfrontier.service.utils.GrpcCleanupExtension
import es.unizar.iaaa.urlfrontier.service.utils.LogCapture
import es.unizar.iaaa.urlfrontier.service.utils.LogCaptureExtension
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Instant
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

@ExtendWith(LogCaptureExtension::class)
class MemoryFrontierServiceTest {
    @RegisterExtension
    @JvmField
    val grpcExtension: GrpcCleanupExtension = GrpcCleanupExtension()

    private lateinit var stub: URLFrontierGrpcKt.URLFrontierCoroutineStub
    private lateinit var service: MemoryFrontierService
    private val testQueueId = queueKey(EXAMPLE_KEY_A, CRAWL_ID_A)

    @BeforeTest
    fun setupServer() {
        service = MemoryFrontierService()
        stub = with(grpcExtension) {
            val serverName = serverName()
            registerServer(serverName, service)
            URLFrontierGrpcKt.URLFrontierCoroutineStub(registerChannel(serverName))
        }
    }

    @Test
    fun `I want to put a url in its queue`() {
        val result = stub.putUrls(URL_DISCOVERED_A_IN_KEY_A_CRAWL_A)
        assertEquals(listOf(EXAMPLE_URL_A), result)
        assertEquals(1, service.queues.size)
        assertEquals(1, service.queues[testQueueId]?.countActive)
        assertEquals(true, service.queues[testQueueId]?.asSequence()?.any { it.url == EXAMPLE_URL_A })
    }

    @Test
    fun `If the key cannot be computed the url is not added to a queue`(logCapture: LogCapture) {
        logCapture.setLogFilter(Level.ERROR)
        val result = stub.putUrls(URL_DISCOVERED_WITHOUT_URL_AND_KEY)
        assertEquals(true, logCapture.lastFormattedMessage?.endsWith("Malformed URL []"))
        assertEquals(listOf(""), result)
        assertEquals(0, service.queues.size)
    }

    @Test
    fun `If I put a discovered url and it exits as discovered in the queue the former it is discarded`(logCapture: LogCapture) {
        logCapture.setLogFilter(Level.DEBUG)
        val result = stub.putUrls(URL_DISCOVERED_A_IN_KEY_A_CRAWL_A, URL_DISCOVERED_A_IN_KEY_A_CRAWL_A)
        assertEquals(
            listOf("Added discovered [$EXAMPLE_URL_A]", "Discarded [$EXAMPLE_URL_A]"),
            logCapture.loggingEvents.map { it.formattedMessage })
        assertEquals(listOf(EXAMPLE_URL_A, EXAMPLE_URL_A), result)
        assertEquals(1, service.queues.size)
        assertEquals(1, service.queues[testQueueId]?.countActive)
        assertEquals(true, service.queues[testQueueId]?.asSequence()?.any { it.url == EXAMPLE_URL_A })
    }

    @Test
    fun `If I put a known url and it exits as discovered in the queue the latter it is discarded`(logCapture: LogCapture) {
        logCapture.setLogFilter(Level.DEBUG)
        val result = stub.putUrls(URL_DISCOVERED_A_IN_KEY_A_CRAWL_A, URL_KNOWN_REFETCHABLE)
        assertEquals(
            listOf("Added discovered [$EXAMPLE_URL_A]", "Removed [$EXAMPLE_URL_A]", "Added known [$EXAMPLE_URL_A]"),
            logCapture.loggingEvents.map { it.formattedMessage })
        assertEquals(listOf(EXAMPLE_URL_A, EXAMPLE_URL_A), result)
        assertEquals(1, service.queues.size)
        assertEquals(1, service.queues[testQueueId]?.countActive)
        assertEquals(true, service.queues[testQueueId]?.asSequence()?.any { it.url == EXAMPLE_URL_A })
    }

    @Test
    fun `If I put a discovered url and it exits as known in the queue the former it is discarded`(logCapture: LogCapture) {
        logCapture.setLogFilter(Level.DEBUG)
        val result = stub.putUrls(URL_KNOWN_REFETCHABLE, URL_DISCOVERED_A_IN_KEY_A_CRAWL_A)
        assertEquals(
            listOf("Added known [$EXAMPLE_URL_A]", "Discarded [$EXAMPLE_URL_A]"),
            logCapture.loggingEvents.map { it.formattedMessage })
        assertEquals(listOf(EXAMPLE_URL_A, EXAMPLE_URL_A), result)
        assertEquals(1, service.queues.size)
        assertEquals(1, service.queues[testQueueId]?.countActive)
        assertEquals(true, service.queues[testQueueId]?.asSequence()?.any { it.url == EXAMPLE_URL_A })
    }

    @Test
    fun `If I put a known url and it exits as known in the queue the former it is discarded`(logCapture: LogCapture) {
        logCapture.setLogFilter(Level.DEBUG)
        val result = stub.putUrls(URL_KNOWN_REFETCHABLE, URL_KNOWN_REFETCHABLE)
        assertEquals(
            listOf("Added known [$EXAMPLE_URL_A]", "Removed [$EXAMPLE_URL_A]", "Added known [$EXAMPLE_URL_A]"),
            logCapture.loggingEvents.map { it.formattedMessage })
        assertEquals(listOf(EXAMPLE_URL_A, EXAMPLE_URL_A), result)
        assertEquals(1, service.queues.size)
        assertEquals(1, service.queues[testQueueId]?.countActive)
        assertEquals(true, service.queues[testQueueId]?.asSequence()?.any { it.url == EXAMPLE_URL_A })
    }

    @Test
    fun `If I put a known url non refetchable it is considered as completed`(logCapture: LogCapture) {
        logCapture.setLogFilter(Level.DEBUG)
        val result = stub.putUrls(URL_KNOWN_NON_REFETCHABLE)
        assertEquals(listOf("Completed [$EXAMPLE_URL_A]"), logCapture.loggingEvents.map { it.formattedMessage })
        assertEquals(listOf(EXAMPLE_URL_A), result)
        assertEquals(1, service.queues.size)
        assertEquals(0, service.queues[testQueueId]?.countActive)
        assertEquals(1, service.queues[testQueueId]?.countCompleted)
    }

    @Test
    fun `I can set the delay for a queue`() {
        stub.putUrls(URL_KNOWN_REFETCHABLE)
        runBlocking {
            stub.setDelay(EXAMPLE_KEY_A, 100, CRAWL_ID_A)
        }
        assertEquals(100, service.queues[testQueueId]?.delay)
    }

    @Test
    fun `Setting the delay for a missing queue is logged as error`(logCapture: LogCapture) {
        logCapture.setLogFilter(Level.DEBUG)
        runBlocking {
            stub.setDelay(EXAMPLE_KEY_A, 100, CRAWL_ID_A)
        }
        assertEquals("Requested missing queue ${CRAWL_ID_A}_$EXAMPLE_KEY_A", logCapture.lastFormattedMessage)
    }

    @Test
    fun `I want to get urls from any queue`() {
        stub.putUrls(URL_DISCOVERED_A_IN_KEY_A_CRAWL_A, URL_DISCOVERED_D_IN_KEY_B_CRAWL_A)
        val result = runBlocking {
            stub.getURLs(getParams { crawlID = CRAWL_ID_A }).toList()
        }
        assertEquals(2, service.queues.size)
        assertEquals(2, result.size)
        assertContentEquals(listOf(EXAMPLE_URL_A, EXAMPLE_URL_D), result.map { it.url })
    }

    @Test
    fun `I want to get urls from a specific queue`() {
        stub.putUrls(URL_DISCOVERED_A_IN_KEY_A_CRAWL_A, URL_DISCOVERED_D_IN_KEY_B_CRAWL_A)
        val result = runBlocking {
            stub.getURLs(getParams {
                key = EXAMPLE_KEY_A
                crawlID = CRAWL_ID_A
            }).toList()
        }
        assertEquals(2, service.queues.size)
        assertEquals(1, result.size)
        assertContentEquals(listOf(EXAMPLE_URL_A), result.map { it.url })
    }

    @Test
    fun `I want to obtain urls from a maximum number of queues`() {
        stub.putUrls(URL_DISCOVERED_A_IN_KEY_A_CRAWL_A, URL_DISCOVERED_D_IN_KEY_B_CRAWL_A)
        val result = runBlocking {
            stub.getURLs(getParams {
                crawlID = CRAWL_ID_A
                maxQueues = 1
            }).toList()
        }
        assertEquals(2, service.queues.size)
        assertEquals(1, result.size)
        assertContentEquals(listOf(EXAMPLE_URL_A), result.map { it.url })
    }

    @Test
    fun `I want to obtain at most X urls from each queue`() {
        stub.putUrls(
            URL_DISCOVERED_A_IN_KEY_A_CRAWL_A, URL_DISCOVERED_B_IN_KEY_A_CRAWL_A,
            URL_DISCOVERED_C_IN_KEY_B_CRAWL_A, URL_DISCOVERED_D_IN_KEY_B_CRAWL_A
        )
        val result = runBlocking {
            stub.getURLs(getParams {
                crawlID = CRAWL_ID_A
                maxUrlsPerQueue = 1
            }).toList()
        }
        assertEquals(2, service.queues.size)
        assertEquals(2, result.size)
        assertContentEquals(listOf(EXAMPLE_URL_A, EXAMPLE_URL_C).sorted(), result.map { it.url }.sorted())
    }

    @Test
    fun `If the frontier is deactivated none is retrieved`() {
        stub.putUrls(
            URL_DISCOVERED_A_IN_KEY_A_CRAWL_A, URL_DISCOVERED_B_IN_KEY_A_CRAWL_A,
            URL_DISCOVERED_C_IN_KEY_B_CRAWL_A, URL_DISCOVERED_D_IN_KEY_B_CRAWL_A
        )
        val result = runBlocking {
            stub.setActive(false)
            stub.getURLs(getParams {
                crawlID = CRAWL_ID_A
            }).toList()
        }
        assertEquals(2, service.queues.size)
        assertEquals(0, result.size)
    }

    @Test
    fun `I want to list the names of the queues`() {
        stub.putUrls(
            URL_DISCOVERED_A_IN_KEY_A_CRAWL_A, URL_DISCOVERED_B_IN_KEY_A_CRAWL_A,
            URL_DISCOVERED_C_IN_KEY_B_CRAWL_A, URL_DISCOVERED_D_IN_KEY_B_CRAWL_A
        )
        val result = runBlocking {
            stub.listQueues(ALL_QUEUES_CRAWL_ID_A)
        }
        assertEquals(2, result.total)
        assertEquals(2, result.size)
        assertEquals(0, result.start)
        assertContentEquals(
            listOf(EXAMPLE_KEY_A, EXAMPLE_KEY_B),
            (0 until result.valuesCount).map { result.getValues(it) })
    }

    @Test
    fun `I want to get the name of the first queue`() {
        stub.putUrls(
            URL_DISCOVERED_A_IN_KEY_A_CRAWL_A, URL_DISCOVERED_B_IN_KEY_A_CRAWL_A,
            URL_DISCOVERED_C_IN_KEY_B_CRAWL_A, URL_DISCOVERED_D_IN_KEY_B_CRAWL_A
        )
        val result = runBlocking {
            stub.listQueues(ONE_QUEUE_CRAWL_ID_A)
        }
        assertEquals(2, result.total)
        assertEquals(1, result.size)
        assertEquals(0, result.start)
        assertContentEquals(listOf(EXAMPLE_KEY_A), (0 until result.valuesCount).map { result.getValues(it) })
    }

    @Test
    fun `I want to get the name of the second queue`() {
        stub.putUrls(
            URL_DISCOVERED_A_IN_KEY_A_CRAWL_A, URL_DISCOVERED_B_IN_KEY_A_CRAWL_A,
            URL_DISCOVERED_C_IN_KEY_B_CRAWL_A, URL_DISCOVERED_D_IN_KEY_B_CRAWL_A
        )
        val result = runBlocking {
            stub.listQueues(ALL_QUEUES_CRAWL_ID_A_START_1)
        }
        assertEquals(2, result.total)
        assertEquals(1, result.size)
        assertEquals(1, result.start)
        assertContentEquals(listOf(EXAMPLE_KEY_B), (0 until result.valuesCount).map { result.getValues(it) })
    }

    @Test
    fun `I want to get the name of the crawls`() {
        stub.putUrls(
            URL_DISCOVERED_A_IN_KEY_A_CRAWL_A, URL_DISCOVERED_B_IN_KEY_A_CRAWL_A,
            URL_DISCOVERED_C_IN_KEY_B_CRAWL_B, URL_DISCOVERED_D_IN_KEY_B_CRAWL_B
        )
        val result = runBlocking {
            stub.listCrawls(empty())
        }
        assertEquals(2, result.valuesCount)
        assertContentEquals(listOf(CRAWL_ID_A, CRAWL_ID_B), (0 until result.valuesCount).map { result.getValues(it) })
    }

    @Test
    fun `I want to remove a crawl`() {
        stub.putUrls(
            URL_DISCOVERED_A_IN_KEY_A_CRAWL_A, URL_DISCOVERED_B_IN_KEY_B_CRAWL_A,
            URL_DISCOVERED_C_IN_KEY_B_CRAWL_B, URL_DISCOVERED_D_IN_KEY_B_CRAWL_B
        )
        runBlocking {
            assertEquals(2, stub.listQueues(ALL_QUEUES_CRAWL_ID_A).size)
            assertEquals(1, stub.listQueues(ALL_QUEUES_CRAWL_ID_B).size)

            assertEquals(2, stub.deleteCrawl(string(CRAWL_ID_A)).value)

            assertEquals(0, stub.listQueues(ALL_QUEUES_CRAWL_ID_A).size)
            assertEquals(1, stub.listQueues(ALL_QUEUES_CRAWL_ID_B).size)

            assertEquals(-1, stub.deleteCrawl(string(CRAWL_ID_A)).value)
        }
    }

    @Test
    fun `I want to block a queue for 1 second`() {
        stub.putUrls(URL_DISCOVERED_A_IN_KEY_A_CRAWL_A, URL_DISCOVERED_D_IN_KEY_B_CRAWL_A)
        runBlocking {
            stub.blockQueueUntil(blockQueueParams {
                key = EXAMPLE_KEY_A
                crawlID = CRAWL_ID_A
                time = Instant.now().epochSecond + 1
            })

            val result1 = stub.getURLs(getParams {
                crawlID = CRAWL_ID_A
            }).toList()
            assertEquals(1, result1.size)
            assertContentEquals(listOf(EXAMPLE_URL_D), result1.map { it.url })

            delay(2000)

            val result2 = stub.getURLs(getParams {
                crawlID = CRAWL_ID_A
            }).toList()
            assertEquals(1, result2.size)
            assertContentEquals(listOf(EXAMPLE_URL_A), result2.map { it.url })
        }
    }

    @Test
    fun `I want to remove a queue`() {
        stub.putUrls(URL_DISCOVERED_A_IN_KEY_A_CRAWL_A, URL_DISCOVERED_B_IN_KEY_B_CRAWL_A)
        runBlocking {
            assertEquals(2, stub.listQueues(ALL_QUEUES_CRAWL_ID_A).size)
            assertEquals(1, stub.deleteQueue(queueWithinCrawlParams {
                crawlID = CRAWL_ID_A
                key = EXAMPLE_KEY_A
            }).value)
            assertEquals(1, stub.listQueues(ALL_QUEUES_CRAWL_ID_A).size)
        }
    }
}
