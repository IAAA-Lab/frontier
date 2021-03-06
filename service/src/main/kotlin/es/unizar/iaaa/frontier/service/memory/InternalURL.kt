package es.unizar.iaaa.frontier.service.memory

import crawlercommons.urlfrontier.Urlfrontier.URLInfo

class InternalURL(
    info: URLInfo,
    val nextFetchDate: Long
) : Comparable<InternalURL> {
    val url: String = info.url
    val crawlID: String = info.crawlID
    val serialised: ByteArray = info.toByteArray()

    /**
     * this is set when the URL is sent for processing
     * so that a subsequent call to getURLs does not send it again
     */
    var heldUntil: Long = -1L

    override operator fun compareTo(other: InternalURL): Int =
        when (val comp = nextFetchDate.compareTo(other.nextFetchDate)) {
            0 -> url.compareTo(other.url)
            else -> comp
        }

    override fun equals(other: Any?): Boolean = url == (other as? InternalURL)?.url

    override fun hashCode(): Int = url.hashCode()

    override fun toString(): String =
        "InternalURL(nextFetchDate=$nextFetchDate, url=$url, serialised=${serialised.contentToString()}, crawlID=$crawlID, heldUntil=$heldUntil)"
}
