package es.unizar.iaaa.urlfrontier.service.memory

import crawlercommons.urlfrontier.Urlfrontier
import mu.KotlinLogging
import java.net.URI
import java.time.Instant

private val logger = KotlinLogging.logger {}

/**
 * Returns the key if any, whether it is a discovered URL or not and an internal
 * object to represent it
 **/
fun Urlfrontier.URLItem.toURLItemRecord(): URLItemRecord {
    val isNew = hasDiscovered()
    val info = if (isNew) discovered.info else known.info
    val nextFetchDate = if (isNew) Instant.now().epochSecond else known.refetchableFromDate
    val key = info.key.ifEmpty {
        logger.debug { "key missing for ${info.url}" }
        runCatching {
            URI.create(info.url).host
        }.getOrDefault(null)
    }

    return when {
        key == null -> URLItemRecordFailure(info) { "Malformed URL [${info.url}]" }
        key.length > 255 -> URLItemRecordFailure(info) { "Key too long [${info.url}]" }
        nextFetchDate < 0 -> URLItemRecordFailure(info) { "Next fetch date wrong [${info.url}]" }
        else -> URLItemRecordSuccess(key, isNew, info, nextFetchDate)
    }
}

sealed interface URLItemRecord {
    val info: Urlfrontier.URLInfo
}

data class URLItemRecordSuccess(
    val key: String,
    val discovered: Boolean,
    override val info: Urlfrontier.URLInfo,
    val nextFetchDate: Long,
) : URLItemRecord

data class URLItemRecordFailure(
    override val info: Urlfrontier.URLInfo,
    val msg: () -> Any?
) : URLItemRecord
