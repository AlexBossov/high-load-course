package ru.quipy.payments.config

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.payments.logic.ExternalServiceProperties
import ru.quipy.payments.logic.PaymentExternalServiceImpl
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

@Service
class AccountBalancer {

    companion object {
        // Ниже приведены готовые конфигурации нескольких аккаунтов провайдера оплаты.
        // Заметьте, что каждый аккаунт обладает своими характеристиками и стоимостью вызова.

        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        private val accountProps_1 = ExternalServiceProperties(
            // most expensive. Call costs 100
            "test",
            "default-1",
            parallelRequests = 10000,
            rateLimitPerSec = 100,
            request95thPercentileProcessingTime = Duration.ofMillis(1000),
        )

        private val accountProps_2 = ExternalServiceProperties(
            // Call costs 70
            "test",
            "default-2",
            parallelRequests = 100,
            rateLimitPerSec = 30,
            request95thPercentileProcessingTime = Duration.ofMillis(10_000),
        )

        private val accountProps_3 = ExternalServiceProperties(
            // Call costs 40
            "test",
            "default-3",
            parallelRequests = 30,
            rateLimitPerSec = 8,
            request95thPercentileProcessingTime = Duration.ofMillis(10_000),
        )

        private val accountProps_4 = ExternalServiceProperties(
            // Call costs 30
            "test",
            "default-4",
            parallelRequests = 8,
            rateLimitPerSec = 5,
            request95thPercentileProcessingTime = Duration.ofMillis(10_000),
        )

        private val maxRequests4 =
            accountProps_4.parallelRequests * 80 / accountProps_4.request95thPercentileProcessingTime.seconds
        private val maxRequests3 =
            accountProps_3.parallelRequests * 80 / accountProps_3.request95thPercentileProcessingTime.seconds + maxRequests4
        private val maxRequests2 =
            accountProps_2.parallelRequests * 80 / accountProps_2.request95thPercentileProcessingTime.seconds + maxRequests3

        private val accounts: Map<ExternalServiceProperties, MyRateLimiter> = mapOf(
            accountProps_1 to MyRateLimiter(accountProps_1.rateLimitPerSec, accountProps_1.parallelRequests),
            accountProps_2 to MyRateLimiter(accountProps_2.rateLimitPerSec, accountProps_2.parallelRequests),
            accountProps_3 to MyRateLimiter(accountProps_3.rateLimitPerSec, accountProps_3.parallelRequests),
            accountProps_4 to MyRateLimiter(accountProps_4.rateLimitPerSec, accountProps_4.parallelRequests)
        )

        private val requestCounter = AtomicLong(0)
    }

    fun getAccount(): ExternalServiceProperties {

        val count = requestCounter.incrementAndGet()
        while (true) {
            if (count <= maxRequests4) {
                accounts[accountProps_4]!!.tickBlocking()
                logger.error("Account 4 acquired")
                return accountProps_4
            } else if (count <= maxRequests3) {
                accounts[accountProps_3]!!.tickBlocking()
                logger.error("Account 3 acquired")
                return accountProps_3
            } else if (count <= maxRequests2) {
                accounts[accountProps_2]!!.tickBlocking()
                logger.error("Account 2 acquired")
                return accountProps_2
            } else {
                accounts[accountProps_1]!!.tickBlocking()
                logger.error("Account 1 acquired")
                return accountProps_1
            }
        }
    }

    fun resize(accountProps: ExternalServiceProperties) {
        requestCounter.decrementAndGet()
        accounts[accountProps]!!.release()
    }
}