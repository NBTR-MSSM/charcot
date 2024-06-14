package org.mountsinaicharcot.fulfillment.configuration

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class FulfillmentConfiguration {
  @Bean("singleThreaded")
  ExecutorService singleThreadedExecutor() {
    return Executors.newSingleThreadExecutor()
  }
}
