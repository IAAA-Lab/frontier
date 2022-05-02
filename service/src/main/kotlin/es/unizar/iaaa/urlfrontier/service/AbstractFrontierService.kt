package es.unizar.iaaa.urlfrontier.service

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import crawlercommons.urlfrontier.URLFrontierGrpc.URLFrontierImplBase
import crawlercommons.urlfrontier.Urlfrontier
import io.grpc.stub.StreamObserver
import io.prometheus.client.Counter
import io.prometheus.client.Summary
import mu.KLoggable
import mu.KLogger
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.Collections

abstract class AbstractFrontierService<T> : URLFrontierImplBase(), KLoggable {
    override val logger: KLogger by lazy { logger() }

    @Suppress("MemberVisibilityCanBePrivate")
    protected var isActive = true
        private set
    internal var defaultDelayForQueues = 1
        private set

    // in memory map of metadata for each queue
    internal val queues: MutableMap<QueueInCrawlKey, QueueInterface<T>> =
        Collections.synchronizedMap(LinkedHashMap<QueueInCrawlKey, QueueInterface<T>>())

    override fun listCrawls(
        request: Urlfrontier.Empty,
        responseObserver: StreamObserver<Urlfrontier.StringList>
    ) {
        val crawlIDs = synchronized(queues) { queues.keys.map { it.crawlID }.distinct().asIterable() }
        responseObserver.onNext(stringList(crawlIDs))
        responseObserver.onCompleted()
    }

    override fun deleteCrawl(
        crawlID: Urlfrontier.String,
        responseObserver: StreamObserver<Urlfrontier.Integer>
    ) {
        val normalisedCrawlID: String = crawlID.value.normaliseCrawlID()
        val total = synchronized(queues) {
            val toDelete = queues.keys.filter { it.crawlID == normalisedCrawlID }.distinct()
            if (toDelete.isEmpty()) -1 else toDelete.sumOf { queues.remove(it)?.countActive ?: 0 }
        }
        responseObserver.onNext(integer(total))
        responseObserver.onCompleted()
    }

    override fun setActive(
        request: Urlfrontier.Boolean,
        responseObserver: StreamObserver<Urlfrontier.Empty>
    ) {
        isActive = request.state
        responseObserver.onNextEmpty()
        responseObserver.onCompleted()
    }

    override fun getActive(request: Urlfrontier.Empty, responseObserver: StreamObserver<Urlfrontier.Boolean>) {
        responseObserver.onNext(Urlfrontier.Boolean.newBuilder().setState(isActive).build())
        responseObserver.onCompleted()
    }

    override fun listQueues(
        request: Urlfrontier.Pagination,
        responseObserver: StreamObserver<Urlfrontier.QueueList>
    ) {
        // max number of values; defaults to 100
        val maxQueues = if (request.size > 0) request.size else 100
        // position of the first result in the list; defaults to 0
        val start = if (request.start > 0) request.start else 0
        // include inactive queues; defaults to false
        val includeInactive = request.includeInactive

        val crawlID = request.crawlID.normaliseCrawlID()
        val now = Instant.now().epochSecond

        logger.info {
            "Received request to list queues [size $maxQueues; start $start; inactive $includeInactive]"
        }

        val (total, list) = synchronized(queues) {
            val queuesInCrawl = queues.asSequence().filter { it.key.crawlID == crawlID }
            queuesInCrawl.count().toLong() to
                queuesInCrawl.withIndex()
                    .drop(start)
                    .filter { (_, entry) -> includeInactive || entry.value.blockedUntil < now }
                    .filter { (_, entry) -> includeInactive || entry.value.countActive > 0 }
                    .map { (idx, entry) -> idx to entry.key.queueKey }
                    .take(maxQueues)
                    .toList()
        }
        val queueList = queueList {
            // total number of queues
            this.total = total
            // number of values returned
            size = list.size
            // position of the first result in the list
            this.start = if (list.isNotEmpty()) list.first().first else -1
            this.crawlID = request.crawlID
            addAllValues(list.map { (_, key) -> key })
        }
        responseObserver.onNext(queueList)
        responseObserver.onCompleted()
    }

    override fun blockQueueUntil(
        request: Urlfrontier.BlockQueueParams,
        responseObserver: StreamObserver<Urlfrontier.Empty>
    ) {
        val qwc = queueKey(request.key, request.crawlID)
        queues[qwc]?.let {
            it.blockedUntil = request.time
        }
        responseObserver.onNext(empty())
        responseObserver.onCompleted()
    }

    override fun setDelay(request: Urlfrontier.QueueDelayParams, responseObserver: StreamObserver<Urlfrontier.Empty>) {
        if (request.key.isEmpty()) {
            defaultDelayForQueues = request.delayRequestable
        } else {
            val qwc = queueKey(request.key, request.crawlID)
            val queue = synchronized(queues) {
                queues.getOrDebug(qwc) { "Requested missing queue $qwc" }
            }
            queue?.delay = request.delayRequestable
        }

        responseObserver.onNextEmpty()
        responseObserver.onCompleted()
    }

    /**
     * Delete  the queue based on the key in parameter.
     */
    override fun deleteQueue(
        request: Urlfrontier.QueueWithinCrawlParams,
        responseObserver: StreamObserver<Urlfrontier.Integer>
    ) {
        val qwc = queueKey(request.key, request.crawlID)
        val q = queues.remove(qwc)?.countActive ?: -1
        responseObserver.onNextInteger(q)
        responseObserver.onCompleted()
    }

    override fun getStats(
        request: Urlfrontier.QueueWithinCrawlParams,
        responseObserver: StreamObserver<Urlfrontier.Stats>
    ) {
        logger.info("Received stats request")
        val normalisedCrawlID: String = request.crawlID.normaliseCrawlID()

        val selectedQueues = if (request.key.isNotEmpty()) {
            // specific queue?
            val qwc = queueKey(request.key, normalisedCrawlID)
            queues[qwc]?.let { listOf(it) } ?: emptyList()
        } else {
            synchronized(queues) {
                queues.asSequence()
                    .filter { (qwc, _) -> qwc.crawlID == normalisedCrawlID }
                    .map { (_, value) -> value }
                    .toList()
            }
        }
        // backed by the queues so can result in a
        // ConcurrentModificationException
        val now = Instant.now().epochSecond
        val stats = synchronized(queues) {
            selectedQueues.fold(Statistics(normalisedCrawlID)) { acc, queue ->
                val inProcessForQueue = queue.getInProcess(now)
                val activeForQueue = queue.countActive
                val isQueueActive = if (inProcessForQueue > 0 || activeForQueue > 0) 1 else 0
                acc.copy(
                    inProcess = acc.inProcess + inProcessForQueue,
                    numQueues = acc.numQueues + 1,
                    active = acc.active + activeForQueue,
                    completed = acc.completed + queue.countCompleted,
                    activeQueues = acc.activeQueues + isQueueActive
                )
            }
        }

        responseObserver.onNextStats(stats)
        responseObserver.onCompleted()
    }

    override fun getURLs(request: Urlfrontier.GetParams, responseObserver: StreamObserver<Urlfrontier.URLInfo>) {
        // on hold
        if (!isActive) {
            responseObserver.onCompleted()
            return
        }
        getURLsCallsTotalCounter.inc()
        val requestTimer = getURLsLatency.startTimer()

        val maxQueues = if (request.maxQueues <= 0) Int.MAX_VALUE else request.maxQueues
        val maxURLsPerQueue = if (request.maxUrlsPerQueue <= 0) Int.MAX_VALUE else request.maxUrlsPerQueue
        val secsUntilRequestable = if (request.delayRequestable <= 0) 30 else request.delayRequestable
        logger.info {
            "Received request to get fetchable URLs [max queues $maxQueues, max URLs $maxURLsPerQueue, delay $secsUntilRequestable]"
        }
        val start = System.currentTimeMillis()

        val crawlID = if (request.hasCrawlID()) request.crawlID.normaliseCrawlID() else null
        val key = request.key
        val now = Instant.now().epochSecond

        val queuesToProcess: List<Pair<QueueInCrawlKey, QueueInterface<T>>> =
            if (!key.isNullOrEmpty() && !crawlID.isNullOrEmpty()) {
                val qwc = queueKey(key, crawlID)
                queues[qwc]?.let { queue -> listOf(qwc to queue) } ?: emptyList()
            } else {
                queues.toList()
            }

        if (queuesToProcess.isEmpty()) {
            logger.info("No queues to get URLs from!")
            responseObserver.onCompleted()
            return
        }

        val queuesSent = queuesToProcess.asSequence().map { (currentCrawlQueue, currentQueue) ->
            synchronized(queues) {
                queues.remove(currentCrawlQueue)
                queues.put(currentCrawlQueue, currentQueue)
            }
            currentCrawlQueue to currentQueue
        }
            .filter { (currentCrawlQueue, _) -> crawlID == null || currentCrawlQueue.crawlID == crawlID }
            .filter { (_, currentQueue) -> currentQueue.blockedUntil < now }
            .filter { (_, currentQueue) ->
                val delay = if (currentQueue.delay == -1) defaultDelayForQueues else currentQueue.delay
                currentQueue.lastProduced + delay < now
            }.filter { (_, currentQueue) -> currentQueue.getInProcess(now) < maxURLsPerQueue }
            .map { (currentCrawlQueue, currentQueue) ->
                val command = SendURLsCommand(
                    queue = currentQueue,
                    prefixedKey = currentCrawlQueue,
                    maxURLsPerQueue = maxURLsPerQueue,
                    secsUntilRequestable = secsUntilRequestable,
                    now = now
                )
                currentQueue to sendURLsForQueue(command, responseObserver)
            }
            .filter { (_, sentForQ) -> sentForQ > 0 }
            .take(maxQueues).toList()
            .map { (currentQueue, sentForQ) ->
                currentQueue.lastProduced = now
                sentForQ
            }

        val totalSent = queuesSent.sum()
        val numQueuesSent = queuesSent.size
        logger.info {
            "Sent $totalSent from $numQueuesSent queue(s) in ${System.currentTimeMillis() - start} msec"
        }
        getURLsTotalCounter.inc(totalSent.toDouble())
        requestTimer.observeDuration()
        responseObserver.onCompleted()
    }

    protected abstract fun sendURLsForQueue(
        sendURLsCommand: SendURLsCommand<T>,
        responseObserver: StreamObserver<Urlfrontier.URLInfo>
    ): Int

    data class SendURLsCommand<T>(
        val queue: QueueInterface<T>,
        val prefixedKey: QueueInCrawlKey,
        val maxURLsPerQueue: Int,
        val secsUntilRequestable: Int,
        val now: Long
    )

    override fun setLogLevel(request: Urlfrontier.LogLevelParams, responseObserver: StreamObserver<Urlfrontier.Empty>) {
        val loggerContext = LoggerFactory.getILoggerFactory() as LoggerContext
        loggerContext.getLogger(request.getPackage()).apply {
            level = Level.toLevel(request.level.toString())
        }
        logger.info {
            "Log level for ${request.getPackage()} set to ${request.level}"
        }
        responseObserver.onNextEmpty()
        responseObserver.onCompleted()
    }

    companion object {
        private val getURLsCallsTotalCounter: Counter = Counter.build()
            .name("frontier_getURLs_calls_total")
            .help("Number of times getURLs has been called.")
            .register()
        private val getURLsTotalCounter: Counter = Counter.build()
            .name("frontier_getURLs_total")
            .help("Number of URLs returned.")
            .register()
        private val getURLsLatency = Summary.build()
            .name("frontier_getURLs_latency_seconds")
            .help("getURLs latency in seconds.")
            .register()
        protected val putURLs_calls: Counter = Counter.build()
            .name("frontier_putURLs_calls_total")
            .help("Number of times putURLs has been called.")
            .register()
        protected val putURLs_urls_count: Counter = Counter.build()
            .name("frontier_putURLs_total")
            .help("Number of URLs sent to the Frontier")
            .register()
        protected val putURLs_discovered_count: Counter = Counter.build()
            .name("frontier_putURLs_discovered_total")
            .help("Count of discovered URLs sent to the Frontier")
            .labelNames("discovered")
            .register()
        protected val putURLs_alreadyknown_count: Counter = Counter.build()
            .name("frontier_putURLs_ignored_total")
            .help("Number of discovered URLs already known to the Frontier")
            .register()
        protected val putURLs_completed_count: Counter = Counter.build()
            .name("frontier_putURLs_completed_total")
            .help("Number of completed URLs")
            .register()
    }
}


context(KLoggable)
fun <K, V> Map<K, V>.getOrDebug(key: K, msg: () -> Any?): V? =
    get(key) ?: null.also { logger.debug(msg) }

fun StreamObserver<Urlfrontier.Empty>.onNextEmpty() {
    onNext(Urlfrontier.Empty.getDefaultInstance())
}

fun StreamObserver<Urlfrontier.Integer>.onNextInteger(value: Long) {
    onNext(Urlfrontier.Integer.newBuilder().setValue(value).build())
}

fun StreamObserver<Urlfrontier.Integer>.onNextInteger(value: Int) {
    onNextInteger(value.toLong())
}

fun StreamObserver<Urlfrontier.Stats>.onNextStats(value: Statistics) {
    val stats = with(value) {
        Urlfrontier.Stats.newBuilder()
            .setNumberOfQueues(numQueues.toLong())
            .setSize(active.toLong())
            .setInProcess(inProcess)
            .putAllCounts(
                mapOf(
                    "completed" to completed,
                    "active_queues" to activeQueues
                )
            )
            .setCrawlID(crawlId)
            .build()
    }
    onNext(stats)
}

data class Statistics(
    val crawlId: String,
    val numQueues: Int = 0,
    val active: Int = 0,
    val inProcess: Int = 0,
    val completed: Long = 0,
    val activeQueues: Long = 0
)
