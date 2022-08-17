package org.mountsinaicharcot.fulfillment.configuration

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@Configuration
class FulfillmentConfiguration {
  @Bean("singleThreaded")
  ExecutorService singleThreadedExecutor() {
    return Executors.newSingleThreadExecutor();
  }
}
