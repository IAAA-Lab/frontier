package es.unizar.iaaa.urlfrontier.service

import crawlercommons.urlfrontier.Urlfrontier
import crawlercommons.urlfrontier.Urlfrontier.Empty
import crawlercommons.urlfrontier.Urlfrontier.Integer
import crawlercommons.urlfrontier.Urlfrontier.QueueList
import crawlercommons.urlfrontier.Urlfrontier.QueueWithinCrawlParams
import crawlercommons.urlfrontier.Urlfrontier.Stats
import crawlercommons.urlfrontier.Urlfrontier.StringList
import crawlercommons.urlfrontier.Urlfrontier.URLInfo

/**
 * Assemble the [Boolean][Urlfrontier.Boolean] message.
 *
 * @param value the value.
 */
fun boolean(value: Boolean): Urlfrontier.Boolean {
    val boolean = Urlfrontier.Boolean.newBuilder()
    boolean.state = value
    return boolean.build()
}

/**
 * Assemble the [Empty] message.
 */
fun empty(): Empty = Empty.getDefaultInstance()

/**
 * Assemble the [Integer] message.
 *
 * @param value the value.
 */
fun integer(value: Int): Integer {
    val integer = Integer.newBuilder()
    integer.value = value.toLong()
    return integer.build()
}

/**
 * Assemble the [QueueList] message.
 */
fun queueList(block: QueueList.Builder.() -> Unit): QueueList {
    val queueList = QueueList.newBuilder()
    block.invoke(queueList)
    return queueList.build()
}

/**
 * Assemble the [QueueWithinCrawlParams] message.
 */
fun queueWithinCrawlParams(block: QueueWithinCrawlParams.Builder.() -> Unit): QueueWithinCrawlParams {
    val queueWithinCrawlParams = QueueWithinCrawlParams.newBuilder()
    block.invoke(queueWithinCrawlParams)
    return queueWithinCrawlParams.build()
}

/**
 * Assemble the [Stats] message.
 */
fun stats(block: Stats.Builder.() -> Unit): Stats {
    val stats = Stats.newBuilder()
    block.invoke(stats)
    return stats.build()
}

/**
 * Assemble the [String][Urlfrontier.String] message.
 */
fun string(value: String): Urlfrontier.String {
    val string = Urlfrontier.String.newBuilder()
    string.value = value
    return string.build()
}

/**
 * Assemble the [StringList] message.
 */
fun stringList(list: Iterable<String>): StringList {
    val stringList = StringList.newBuilder()
    stringList.addAllValues(list)
    return stringList.build()
}

/**
 * Assemble the [URLInfo] message.
 */
fun urlInfo(block: URLInfo.Builder.() -> Unit): URLInfo {
    val urlInfo = URLInfo.newBuilder()
    block.invoke(urlInfo)
    return urlInfo.build()
}
