package ru.quipy.payments.config

import okhttp3.OkHttpClient
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.payments.logic.PaymentExternalServiceImpl


@Configuration
class ExternalServicesConfig(
    private val accountBalancer: AccountBalancer,
    private val client: OkHttpClient
) {
    companion object {
        const val PRIMARY_PAYMENT_BEAN = "PRIMARY_PAYMENT_BEAN"
    }

    @Bean(PRIMARY_PAYMENT_BEAN)
    fun fastExternalService() =
        PaymentExternalServiceImpl(
            accountBalancer,
            client
        )
}