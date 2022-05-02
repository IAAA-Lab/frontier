package es.unizar.iaaa.urlfrontier.service

import crawlercommons.urlfrontier.URLFrontierGrpcKt
import crawlercommons.urlfrontier.Urlfrontier
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking

suspend fun URLFrontierGrpcKt.URLFrontierCoroutineStub.getActive() = getActive(empty()).state
suspend fun URLFrontierGrpcKt.URLFrontierCoroutineStub.setActive(value: Boolean) {
    setActive(boolean(value))
}

suspend fun URLFrontierGrpcKt.URLFrontierCoroutineStub.setDelay(
    key: String,
    delayRequestable: Int,
    crawlID: String
) {
    setDelay(queueDelayParams(key, delayRequestable, crawlID))
}

/**
 * Parameter message for [setDelay][URLFrontierGrpcKt.URLFrontierCoroutineStub.setDelay].
 *
 * @param key  ID for the queue, an empty value sets the default for all the queues.
 * @param delayRequestable delay in seconds before a queue can provide new URLs.
 * @param crawlID crawl ID - empty string for default.
 */
fun queueDelayParams(
    key: String,
    delayRequestable: Int,
    crawlID: String
): Urlfrontier.QueueDelayParams = Urlfrontier.QueueDelayParams.newBuilder().also {
    it.key = key
    it.delayRequestable = delayRequestable
    it.crawlID = crawlID
}.build()

fun empty(): Urlfrontier.Empty = Urlfrontier.Empty.getDefaultInstance()
fun boolean(value: Boolean): Urlfrontier.Boolean = Urlfrontier.Boolean.newBuilder().setState(value).build()
fun string(value: String): Urlfrontier.String = Urlfrontier.String.newBuilder().setValue(value).build()

fun discovered(
    block: Urlfrontier.DiscoveredURLItem.Builder.() -> Unit,
): Urlfrontier.URLItem {
    val discovered = Urlfrontier.DiscoveredURLItem.newBuilder()
    block.invoke(discovered)
    return Urlfrontier.URLItem.newBuilder().setDiscovered(discovered.build()).build()
}

fun known(
    block: Urlfrontier.KnownURLItem.Builder.() -> Unit,
): Urlfrontier.URLItem {
    val known = Urlfrontier.KnownURLItem.newBuilder()
    block.invoke(known)
    return Urlfrontier.URLItem.newBuilder().setKnown(known.build()).build()
}

fun Urlfrontier.DiscoveredURLItem.Builder.info(
    block: Urlfrontier.URLInfo.Builder.() -> Unit,
) {
    val info = Urlfrontier.URLInfo.newBuilder()
    block.invoke(info)
    setInfo(info)
}

fun Urlfrontier.KnownURLItem.Builder.info(
    block: Urlfrontier.URLInfo.Builder.() -> Unit,
) {
    val info = Urlfrontier.URLInfo.newBuilder()
    block.invoke(info)
    setInfo(info)
}

fun blockQueueParams(
    block: Urlfrontier.BlockQueueParams.Builder.() -> Unit,
): Urlfrontier.BlockQueueParams {
    val blockQueueParams = Urlfrontier.BlockQueueParams.newBuilder()
    block.invoke(blockQueueParams)
    return blockQueueParams.build()
}

fun getParams(
    block: Urlfrontier.GetParams.Builder.() -> Unit,
): Urlfrontier.GetParams {
    val getParams = Urlfrontier.GetParams.newBuilder()
    block.invoke(getParams)
    return getParams.build()
}

fun Urlfrontier.GetParams.Builder.anyCrawlID() {
    val info = Urlfrontier.GetParams.newBuilder().anyCrawlIDBuilder
    setAnyCrawlID(info)
}

fun pagination(
    block: Urlfrontier.Pagination.Builder.() -> Unit,
): Urlfrontier.Pagination {
    val pagination = Urlfrontier.Pagination.newBuilder()
    block.invoke(pagination)
    return pagination.build()
}

fun URLFrontierGrpcKt.URLFrontierCoroutineStub.putUrls(vararg elements: Urlfrontier.URLItem): List<String> {
    val request = flowOf(*elements)
    val response = putURLs(request).map { it.value }
    return runBlocking {
        response.toList()
    }
}

