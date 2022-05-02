package es.unizar.iaaa.urlfrontier.service

import crawlercommons.urlfrontier.Urlfrontier

/**
 * Assemble the empty message.
 */
fun empty() = Urlfrontier.Empty.getDefaultInstance()

/**
 * Assemble the integer message.
 */
fun integer(value: Int) : Urlfrontier.Integer {
    val integer = Urlfrontier.Integer.newBuilder()
    integer.value = value.toLong()
    return integer.build()
}

/**
 * Assemble the queue list message.
 */
fun queueList(
    block: Urlfrontier.QueueList.Builder.() -> Unit
): Urlfrontier.QueueList {
    val queueList = Urlfrontier.QueueList.newBuilder()
    block.invoke(queueList)
    return queueList.build()
}

fun queueWithinCrawlParams(
    block: Urlfrontier.QueueWithinCrawlParams.Builder.() -> Unit
): Urlfrontier.QueueWithinCrawlParams {
    val queueWithinCrawlParams = Urlfrontier.QueueWithinCrawlParams.newBuilder()
    block.invoke(queueWithinCrawlParams)
    return queueWithinCrawlParams.build()
}

/**
 * Assemble the string message.
 */
fun string(value: String,): Urlfrontier.String {
    val string = Urlfrontier.String.newBuilder()
    string.value = value
    return string.build()
}

/**
 * Assemble the string list message.
 */
fun stringList(list: Iterable<String>) : Urlfrontier.StringList {
    val stringList = Urlfrontier.StringList.newBuilder()
    stringList.addAllValues(list)
    return stringList.build()
}

