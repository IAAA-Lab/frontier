package es.unizar.iaaa.frontier.service.memory

import es.unizar.iaaa.frontier.service.QueueInterface
import java.util.PriorityQueue

class URLQueue : PriorityQueue<InternalURL>(), QueueInterface<InternalURL> {
    // keep a hash of the completed URLs
    // these won't be refetched
    private val completed = HashSet<String>()
    override var blockedUntil: Long = -1
    override var delay = -1
    override var lastProduced: Long = 0

    override val countCompleted: Int
        get() = completed.size

    override val countActive: Int
        get() = size

    /**
     * a URL in process has a [InternalURL.heldUntil] and is at the beginning of a queue
     */
    override fun getInProcess(now: Long): Int = asSequence()
        .takeWhile { it.heldUntil > now }
        .count()

    /**
     * Been fetched before?
     */
    override operator fun contains(element: InternalURL): Boolean = when {
        completed.contains(element.url) -> true
        else -> super.contains(element)
    }

    fun addToCompleted(url: String) {
        completed.add(url)
    }

    override fun asSequence(): Sequence<InternalURL> = iterator().asSequence()
}
