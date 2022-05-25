package es.unizar.iaaa.frontier.service

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class URLFrontierServerIntegrationTest {

    @Test
    fun `start and shutdown the service`() {
        val server = URLFrontierServer()
        server.port = 7071
        Thread { server.call() }.start()
        while (!server.isRunning) {
            Thread.sleep(1000L)
        }
        assertEquals(7071, server.runtimeConfig?.server?.port)
        assertNull(server.runtimeConfig?.prometheus)
        assertTrue(server.shutdown())
    }

    @Test
    fun `start and shutdown the service with prometheus`() {
        val server = URLFrontierServer()
        server.port = 7071
        server.prometheusPort = 9090
        Thread { server.call() }.start()
        while (!server.isRunning) {
            Thread.sleep(1000L)
        }
        assertEquals(7071, server.runtimeConfig?.server?.port)
        assertEquals(9090, server.runtimeConfig?.prometheus?.port)
        assertTrue(server.shutdown())
    }
}
