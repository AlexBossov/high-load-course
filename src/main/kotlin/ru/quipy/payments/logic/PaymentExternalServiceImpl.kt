package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.config.AccountBalancer
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    private val accountBalancer: AccountBalancer,
    private val client: OkHttpClient
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {

        val account = accountBalancer.getAccount()

        logger.warn("[${account.accountName}] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

        val transactionId = UUID.randomUUID()
        logger.info("[${account.accountName}] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${account.serviceName}&accountName=${account.accountName}&transactionId=$transactionId")
            post(emptyBody)
        }.build()

        val call: Call = client.newCall(request)

        call.enqueue(object : Callback {
            override fun onResponse(call: Call, response: Response) {
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[${account.accountName}] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(false, e.message)
                }

                logger.warn("[${account.accountName}] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }

                logger.warn("[${account.accountName}] Submitting payment request for payment $paymentId. Success: ${now() - paymentStartedAt} ms")
                accountBalancer.resize(account)
            }

            override fun onFailure(call: Call, e: IOException) {
                println(e.message)
                when (e) {
                    is SocketTimeoutException -> {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }

                    else -> {
                        logger.error(
                            "[${account.accountName}] Payment failed for txId: $transactionId, payment: $paymentId",
                            e
                        )

                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }

                logger.warn("[${account.accountName}] Submitting payment request for payment $paymentId. Fail: ${now() - paymentStartedAt} ms")
                accountBalancer.resize(account)
            }
        })
    }
}

fun now() = System.currentTimeMillis()