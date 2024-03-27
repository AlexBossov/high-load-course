package ru.quipy.payments.config

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.CoroutineRateLimiter
import ru.quipy.payments.logic.now
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.math.min

class MyRateLimiter(
    private val rate: Int,
    private val window: Int,
    private val timeUnit: TimeUnit = TimeUnit.SECONDS
) {
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(CoroutineRateLimiter::class.java)
        private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    }

    private val rateSemaphore = java.util.concurrent.Semaphore(rate, true)
    private val windowSemaphore = java.util.concurrent.Semaphore(window, true)

    private var lastUpdatedTs = now()

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            val started = now()
            val prevUpdate = lastUpdatedTs

            val permitsToRelease = min(
                rate - rateSemaphore.availablePermits(),
                windowSemaphore.availablePermits()
            )
            runCatching {
                rateSemaphore.release(permitsToRelease)
            }.onFailure { th -> logger.error("Failed while releasing permits", th) }
            lastUpdatedTs = now()

            logger.debug("Released $permitsToRelease permits. Between updates: ${lastUpdatedTs - prevUpdate} ms")
            delay(timeUnit.toMillis(1) - (now() - started))
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }

    fun tickBlocking() = rateSemaphore.acquire()

    fun release() = windowSemaphore.release()
}