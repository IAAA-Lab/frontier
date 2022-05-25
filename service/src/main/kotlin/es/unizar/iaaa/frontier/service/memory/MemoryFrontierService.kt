package es.unizar.iaaa.frontier.service.memory

import crawlercommons.urlfrontier.Urlfrontier
import crawlercommons.urlfrontier.Urlfrontier.URLInfo
import crawlercommons.urlfrontier.Urlfrontier.URLItem
import es.unizar.iaaa.frontier.service.AbstractFrontierService
import es.unizar.iaaa.frontier.service.queueKey
import es.unizar.iaaa.frontier.service.string
import es.unizar.iaaa.frontier.service.urlInfo
import io.grpc.stub.StreamObserver

/**
 * A simple implementation of a URL Frontier service using in memory data structures.
 */
open class MemoryFrontierService : AbstractFrontierService<InternalURL>() {
    /**
     * @return true if at least one URL has been sent for this queue, false otherwise
     */
    override fun sendURLsForQueue(
        sendURLsCommand: SendURLsCommand<InternalURL>,
        responseObserver: StreamObserver<URLInfo>
    ): Int = with(sendURLsCommand) {
        queue.asSequence()
            .takeWhile { it.nextFetchDate <= now }
            .filter { it.heldUntil <= now }
            .take(maxURLsPerQueue)
            .map {
                responseObserver.onNext(
                    urlInfo {
                        key = prefixedKey.queueKey
                        crawlID = prefixedKey.crawlID
                        url = it.url
                        putAllMetadata(URLInfo.parseFrom(it.serialised).metadataMap)
                    }
                )
                it.heldUntil = now + secsUntilRequestable
            }
            .count()
    }

    override fun putURLs(responseObserver: StreamObserver<Urlfrontier.String>): StreamObserver<URLItem> =
        PutURLsResponse(responseObserver)

    inner class PutURLsResponse(private val responseObserver: StreamObserver<Urlfrontier.String>) :
        StreamObserver<URLItem> {
        override fun onNext(value: URLItem) {
            val record = value.toURLItemRecord()
            when (record) {
                is URLItemRecordSuccess -> {
                    val iu = InternalURL(record.info, record.nextFetchDate)
                    val qk = queueKey(record.key, record.info.crawlID)

                    // get the priority queue or create one
                    val queue = synchronized(queues) {
                        queues.getOrPut(qk) { URLQueue() } as URLQueue
                    }
                    val inQueue = queue.contains(iu)
                    if (!inQueue || !record.discovered) {
                        if (inQueue) {
                            queue.remove(iu)
                            logger.debug { "Removed [${iu.url}]" }
                        }
                        when {
                            record.discovered -> {
                                queue.add(iu)
                                logger.debug { "Added discovered [${iu.url}]" }
                            }
                            iu.nextFetchDate != 0L -> {
                                queue.add(iu)
                                logger.debug { "Added known [${iu.url}]" }
                            }
                            else -> {
                                queue.addToCompleted(iu.url)
                                logger.debug { "Completed [${iu.url}]" }
                            }
                        }
                    } else {
                        logger.debug { "Discarded [${iu.url}]" }
                    }
                }
                is URLItemRecordFailure -> logger.error(record.msg)
            }
            responseObserver.onNext(string(record.info.url))
        }

        override fun onError(t: Throwable) {
            logger.error(t) { "Throwable caught" }
        }

        override fun onCompleted() {
            responseObserver.onCompleted()
        }
    }
}
