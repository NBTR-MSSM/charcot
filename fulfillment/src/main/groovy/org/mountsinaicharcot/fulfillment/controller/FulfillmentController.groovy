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

  @PostMapping('/cerebrum-image-orders/{orderId}/fulfill')
  ResponseEntity fulfill(@PathVariable('orderId') String orderId) {
    log.info "Fulfilling order $orderId"
    try {
      log.info "Contacting DynamoDB for $orderId to retrieve order info..."
      // Contact DynamoDB to get files associated with the order
      OrderInfoDto orderInfoDto = fulfillmentService.retrieveOrderInfo(orderId)
      if (!orderInfoDto || !orderInfoDto.fileNames) {
        return ResponseEntity.notFound().build()
      }

      log.info "Retrieved order info for $orderId, downloading image slides from S3..."
      // Download all the files from S3 into temp folder 'orderId'
      executorService.execute({ ->
        List<String> fileNames = orderInfoDto.fileNames
        int zipCnt = 1
        Map<Integer, List<String>> bucketToFileList = fulfillmentService.partitionFileListIntoBucketsUpToSize(fileNames)
        int totalZips = bucketToFileList.size()
        bucketToFileList.each { Integer bucketNumber, List<String> filesToZip ->
          def startAll = System.currentTimeMillis()
          filesToZip.each { String fileName ->
            def startCurrent = System.currentTimeMillis()
            fulfillmentService.downloadS3Object(orderId, fileName)
            fulfillmentService.downloadS3Object(orderId, fileName.replace('.mrxs', '/'))
            log.info "Took ${System.currentTimeMillis() - startCurrent} milliseconds to download $fileName for request $orderId"
          }
          log.info "Took ${System.currentTimeMillis() - startAll} milliseconds to download all the image slides for request $orderId"

          // Create zip
          String zipName = totalZips > 1 ? "$orderId-$zipCnt-of-${totalZips}.zip" : "${orderId}.zip"
          def startZip = System.currentTimeMillis()
          fulfillmentService.createZip(orderInfoDto, zipName)
          log.info "Took ${System.currentTimeMillis() - startZip} milliseconds to create zip for request $orderId"

          // Upload zip to S3
          def startUpload = System.currentTimeMillis()
          fulfillmentService.uploadObjectToS3(orderInfoDto, zipName)
          log.info "Took ${System.currentTimeMillis() - startUpload} milliseconds to upload zip for request $orderId"

          // Generate a signed URL
          String zipLink = fulfillmentService.generateSignedZipUrl(orderInfoDto, zipName)

          // Send email
          fulfillmentService.sendEmail(orderInfoDto, zipLink, zipCnt, totalZips)

          // cleanup in preparation for next batch, this way
          // we free up space so as to to avoid blowing disk space on the host
          fulfillmentService.cleanUp(orderId, zipName)

          ++zipCnt
        }
      })

      ResponseEntity.accepted().body(orderInfoDto.fileNames)
    } catch (Exception e) {
      log.error "An problem occurred fulfilling $orderId", e
      return ResponseEntity.internalServerError().body("There was a problem: $e")
    }
  }
}
