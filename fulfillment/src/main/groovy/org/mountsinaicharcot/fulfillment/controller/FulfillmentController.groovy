package org.mountsinaicharcot.fulfillment.controller

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.mountsinaicharcot.fulfillment.dto.OrderInfoDto
import org.mountsinaicharcot.fulfillment.service.FulfillmentService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController

import java.util.concurrent.ExecutorService

@RestController
@CompileStatic
@Slf4j
class FulfillmentController {
  @Autowired
  FulfillmentService fulfillmentService

  @Autowired
  ExecutorService executorService

  /**
   * TODO: Complain and deter attempts to fulfill an order already processed
   */
  @PostMapping('/cerebrum-image-orders/{orderId}/fulfill')
  ResponseEntity fulfill(@PathVariable('orderId') String orderId) {
    try {
      OrderInfoDto orderInfoDto = fulfillmentService.retrieveOrderInfo(orderId)
      if (!orderInfoDto) {
        log.info "Order $orderId not found."
        return ResponseEntity.notFound().build()
      }
      executorService.execute({ ->
        fulfillmentService.fulfill(orderInfoDto)
      })
      ResponseEntity.accepted().body("Request $orderId has been accepted for processing")
    } catch (Exception e) {
      log.error "An problem occurred fulfilling $orderId", e
      ResponseEntity.internalServerError().body("There was a problem: $e")
    }
  }
}
