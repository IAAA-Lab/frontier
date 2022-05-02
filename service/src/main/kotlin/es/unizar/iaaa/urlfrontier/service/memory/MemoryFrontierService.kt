package es.unizar.iaaa.urlfrontier.service.memory

import crawlercommons.urlfrontier.Urlfrontier
import crawlercommons.urlfrontier.Urlfrontier.URLInfo
import crawlercommons.urlfrontier.Urlfrontier.URLItem
import es.unizar.iaaa.urlfrontier.service.AbstractFrontierService
import es.unizar.iaaa.urlfrontier.service.queueKey
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
            .map {
                runCatching {
                    responseObserver.onNext(it.toURLInfo(prefixedKey))
                    it.heldUntil = now + secsUntilRequestable
                }.onFailure {
                    logger.error("Caught unlikely error", it)
                }
            }
            .filter { it.isSuccess }
            .take(maxURLsPerQueue)
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
                    val (key, discovered, info, nextFetchDate) = record
                    val iu = InternalURL(info, nextFetchDate)
                    val qk = queueKey(key, iu.crawlID)

                    // get the priority queue or create one
                    val queue = synchronized(queues) {
                        queues.getOrPut(qk) { URLQueue() } as URLQueue
                    }
                    val inQueue = queue.contains(iu)
                    if (!inQueue || !discovered) {
                        if (inQueue) {
                            queue.remove(iu)
                            logger.debug { "Removed [${iu.url}]" }
                        }
                        when {
                            discovered -> {
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
            responseObserver.onNext(
                Urlfrontier.String.newBuilder()
                    .setValue(record.info.url)
                    .build()
            )
        }

        override fun onError(t: Throwable) {
            logger.error(t) { "Throwable caught" }
        }

        override fun onCompleted() {
            responseObserver.onCompleted()
        }
    }
}

