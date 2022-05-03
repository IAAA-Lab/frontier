package es.unizar.iaaa.urlfrontier.service

data class QueueInCrawlKey(
    val queueKey: String,
    val crawlID: String
) : Comparable<QueueInCrawlKey> {

    override operator fun compareTo(other: QueueInCrawlKey): Int {
        val diff = crawlID.compareTo(other.crawlID)
        return if (diff != 0) diff else queueKey.compareTo(other.queueKey)
    }

    override fun toString(): String = "${crawlID}_$queueKey"
}

fun queueKey(
    queueKey: String,
    crawlID: String
): QueueInCrawlKey = QueueInCrawlKey(queueKey, crawlID.ifEmpty { DEFAULT_CRAWL_ID })

internal const val DEFAULT_CRAWL_ID = "DEFAULT"
