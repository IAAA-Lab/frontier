package es.unizar.iaaa.frontier.service.runtime

/**
 * Gets runtime lazily.
 */
val runtime: Runtime by lazy { Runtime.getRuntime() }

/**
 * Add a shutdown hook.
 */
fun Runtime.shutdownHook(hook: () -> Unit): Thread = Thread(hook).also { addShutdownHook(it) }
