package es.unizar.iaaa.frontier.service.utils

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.filter.LevelFilter
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.core.filter.Filter
import ch.qos.logback.core.read.ListAppender
import ch.qos.logback.core.spi.FilterReply
import org.junit.jupiter.api.extension.AfterTestExecutionCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.junit.jupiter.api.extension.ParameterResolver
import org.slf4j.LoggerFactory

class LogCaptureExtension : ParameterResolver, AfterTestExecutionCallback {
    private val logger: Logger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as Logger
    private val logCapture: LogCapture = LogCapture()

    override fun supportsParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext): Boolean =
        parameterContext.parameter.type == LogCapture::class.java

    override fun resolveParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext): Any {
        setup()
        return logCapture
    }

    override fun afterTestExecution(context: ExtensionContext) {
        teardown()
    }

    private fun setup() {
        logger.addAppender(logCapture.listAppender)
        logCapture.setLogFilter(Level.INFO)
        logCapture.start()
    }

    private fun teardown() {
        logger.detachAndStopAllAppenders()
        logCapture.stop()
    }
}

class LogCapture internal constructor() {
    val listAppender: ListAppender<ILoggingEvent> = ListAppender()
    val firstFormattedMessage: String?
        get() = getFormattedMessageAt(0)
    val lastFormattedMessage: String?
        get() = getFormattedMessageAt(listAppender.list.size - 1)

    val loggingEvent: LoggingEvent?
        get() = getLoggingEventAt(0)

    val loggingEvents: List<LoggingEvent>
        get() = listAppender.list.filterIsInstance<LoggingEvent>()

    fun getFormattedMessageAt(index: Int): String? = getLoggingEventAt(index)?.formattedMessage

    fun getLoggingEventAt(index: Int): LoggingEvent? = listAppender.list.getOrNull(index) as LoggingEvent?

    fun setLogFilter(logLevel: Level) {
        listAppender.clearAllFilters()
        listAppender.addFilter(buildLevelFilter(logLevel))
    }

    fun clear() {
        listAppender.list.clear()
    }

    fun start() {
        listAppender.start()
    }

    fun stop() {
        listAppender.stop()
        listAppender.list.clear()
    }

    private fun buildLevelFilter(logLevel: Level): Filter<ILoggingEvent> {
        val levelFilter = LevelFilter()
        levelFilter.setLevel(logLevel)
        levelFilter.onMismatch = FilterReply.DENY
        levelFilter.start()
        return levelFilter
    }
}
