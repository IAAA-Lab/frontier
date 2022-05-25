package es.unizar.iaaa.frontier.service

import crawlercommons.urlfrontier.Urlfrontier
import io.grpc.stub.StreamObserver

object Fixtures {
    const val CRAWL_ID_A = "test_A"
    const val CRAWL_ID_B = "test_B"
    const val EXAMPLE_KEY_A = "key_A"
    const val EXAMPLE_KEY_B = "key_B"
    const val EXAMPLE_URL_A = "https://www.example.com/a"
    const val EXAMPLE_URL_B = "https://www.example.com/b"
    const val EXAMPLE_URL_C = "https://www.example.com/c"
    const val EXAMPLE_URL_D = "https://www.example.com/d"
    const val EMPTY_KEY = ""
    val URL_KNOWN_REFETCHABLE = known {
        info {
            url = EXAMPLE_URL_A
            key = EXAMPLE_KEY_A
            crawlID = CRAWL_ID_A
        }
        refetchableFromDate = 1
    }
    val URL_KNOWN_NON_REFETCHABLE = known {
        info {
            url = EXAMPLE_URL_A
            key = EXAMPLE_KEY_A
            crawlID = CRAWL_ID_A
        }
        refetchableFromDate = 0
    }
    val URL_DISCOVERED_A_IN_KEY_A_CRAWL_A = discovered {
        info {
            url = EXAMPLE_URL_A
            key = EXAMPLE_KEY_A
            crawlID = CRAWL_ID_A
        }
    }
    val URL_DISCOVERED_B_IN_KEY_A_CRAWL_A = discovered {
        info {
            url = EXAMPLE_URL_B
            key = EXAMPLE_KEY_A
            crawlID = CRAWL_ID_A
        }
    }
    val URL_DISCOVERED_B_IN_KEY_B_CRAWL_A = discovered {
        info {
            url = EXAMPLE_URL_B
            key = EXAMPLE_KEY_B
            crawlID = CRAWL_ID_A
        }
    }
    val URL_DISCOVERED_C_IN_KEY_B_CRAWL_A = discovered {
        info {
            url = EXAMPLE_URL_C
            key = EXAMPLE_KEY_B
            crawlID = CRAWL_ID_A
        }
    }
    val URL_DISCOVERED_D_IN_KEY_B_CRAWL_A = discovered {
        info {
            url = EXAMPLE_URL_D
            key = EXAMPLE_KEY_B
            crawlID = CRAWL_ID_A
        }
    }
    val URL_DISCOVERED_C_IN_KEY_B_CRAWL_B = discovered {
        info {
            url = EXAMPLE_URL_C
            key = EXAMPLE_KEY_B
            crawlID = CRAWL_ID_B
        }
    }
    val URL_DISCOVERED_D_IN_KEY_B_CRAWL_B = discovered {
        info {
            url = EXAMPLE_URL_D
            key = EXAMPLE_KEY_B
            crawlID = CRAWL_ID_B
        }
    }
    val URL_DISCOVERED_WITHOUT_URL_AND_KEY = discovered {
        info {
            crawlID = CRAWL_ID_A
        }
    }

    val ALL_QUEUES_CRAWL_ID_A = pagination { crawlID = CRAWL_ID_A }

    val ALL_QUEUES_CRAWL_ID_B = pagination { crawlID = CRAWL_ID_B }

    val ALL_QUEUES_CRAWL_ID_A_START_1 = pagination {
        crawlID = CRAWL_ID_A
        start = 1
    }

    val ONE_QUEUE_CRAWL_ID_A = pagination {
        crawlID = CRAWL_ID_A
        size = 1
    }
}

internal class FixtureAbstractFrontierService : AbstractFrontierService<String>() {
    override fun sendURLsForQueue(
        sendURLsCommand: SendURLsCommand<String>,
        responseObserver: StreamObserver<Urlfrontier.URLInfo>
    ): Int {
        TODO("Not yet implemented")
    }
}
