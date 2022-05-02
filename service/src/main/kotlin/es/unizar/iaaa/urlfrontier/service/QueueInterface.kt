package es.unizar.iaaa.urlfrontier.service

interface QueueInterface<T> {
    var blockedUntil: Long
    var delay: Int
    var lastProduced: Long
    val countCompleted: Int
    val countActive: Int

    fun getInProcess(now: Long): Int
    fun asSequence(): Sequence<T>
}
