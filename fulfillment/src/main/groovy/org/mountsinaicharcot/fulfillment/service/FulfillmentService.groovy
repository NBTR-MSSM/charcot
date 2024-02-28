package org.mountsinaicharcot.fulfillment.service

import com.amazonaws.auth.profile.ProfileCredentialsProvider as ProfileCredentialsProviderV1
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectListing
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder
import com.amazonaws.services.simpleemail.model.Body
import com.amazonaws.services.simpleemail.model.Content
import com.amazonaws.services.simpleemail.model.Destination
import com.amazonaws.services.simpleemail.model.Message
import com.amazonaws.services.simpleemail.model.SendEmailRequest
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.amazonaws.services.sqs.model.Message as SQSMessage
import com.amazonaws.services.sqs.model.ReceiveMessageRequest
import groovy.json.JsonSlurper
import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import groovy.transform.ToString
import groovy.util.logging.Slf4j
import java.nio.charset.Charset
import java.nio.file.Paths
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.mountsinaicharcot.fulfillment.dto.OrderInfoDto
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.stereotype.Service
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.FileDownload
import software.amazon.awssdk.transfer.s3.model.FileUpload
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest
import software.amazon.awssdk.utils.Pair

@Service
@Slf4j
@CompileStatic
class FulfillmentService implements CommandLineRunner {
  @Value('${charcot.sqs.order.queue.url}')
  String sqsOrderQueueUrl

  @Value('${charcot.dynamodb.order.table.name}')
  String dynamoDbOrderTableName

  @Value('${charcot.dynamodb.image.metadata.table.name}')
  String dynamoDbImageMetadataTableName

  // This is the source bucket where the image files are stored
  @Value('${charcot.s3.odp.bucket.name}')
  String s3OdpBucketName

  // This is the target bucket where the Zip files get stored
  @Value('${charcot.s3.zip.bucket.name}')
  String s3ZipBucketName

  @Value('${charcot.profile.name.odp:mssm-odp}')
  String odpProfileName

  @Value('${charcot.is.local:false}')
  boolean local

  @Value('${charcot.ses.from.email}')
  String fromEmail

  @Value('${spring.profiles.active}')
  String activeProfile

  final private static String WORK_FOLDER = './.charcot'

  final private static Long FILE_BUCKET_SIZE = 50000000000

  final private static List<String> NUMBER_ATTRIBUTES = ['subjectNumber', 'age']

  final private static List<String> STRING_ATTRIBUTES = [
    'race',
    'diagnosis',
    'sex',
    'region',
    'stain',
    'fileName'
  ]

  // DynamoDB's limit on the size of each record is 400KB, but set to 300KB to allow some buffer
  final private static Integer DYNAMODB_MAX_ITEM_SIZE_IN_BYTES = 300000


  /**
   * After Spring application context starts up, set up an infinite loop of polling SQS for new messages*/
  void run(String... args) throws Exception {
    log.info "Entering queue poll loop"
    while (true) {
      Map<String, String> orderInfoFromSqs
      OrderInfoDto orderInfo
      try {
        orderInfoFromSqs = retrieveNextOrderId()
        if (!orderInfoFromSqs) {
          continue
        }
        orderInfo = retrieveOrderInfo(orderInfoFromSqs.orderId)
        orderInfo.sqsReceiptHandle = orderInfoFromSqs.sqsReceiptHandle
        updateSqsReceiptHandle(orderInfo.orderId, orderInfo.recordNumber, orderInfo.sqsReceiptHandle)
        if (orderInfo.status != 'received') {
          /*
           * Another worker already processed or is processing this order. If the request is large,
           * the AWS SQS max visibility timeout window of 12 hours can/will be exhausted and another worker will see
           * the message again, which would result in duplicate work on this order. Our escape hatch for
           * that is to rely on order status to know whenever the worker is done processing the order. Also this fetch has
           * the effect of extending the visibility timeout by another 12 hours on behalf of the worker
           * handling this request. One caveat is that we have to record the "refreshed" receipt handle because the
           * previous one has now gone stale.
           */
          continue
        }
        fulfill(orderInfo)
      } catch (Exception e) {
        log.error "Problem fulfilling $orderInfoFromSqs.orderId", e
        updateOrderStatus(orderInfo, 'failed', e.toString())
      }
    }
  }

  boolean cancelIfRequested(OrderInfoDto orderInfo) {
    if (orderInfo.status == 'cancel-requested') {
      updateOrderStatus(orderInfo, 'canceled')
      return true
    }
    false
  }

  private static String currentTime() {
    DateTimeZone utc = DateTimeZone.forID('GMT')
    DateTime dt = new DateTime(utc)
    DateTimeFormatter fmt = DateTimeFormat.forPattern('E, d MMM, yyyy HH:mm:ssz')
    StringBuilder now = new StringBuilder()
    fmt.printTo(now, dt)
    now.toString()
  }

  void fulfill(OrderInfoDto orderInfo) {
    systemStats()
    String orderId = orderInfo.orderId
    log.info "Fulfilling order ${orderInfo.toString()}"
    updateOrderStatus(orderInfo, 'processing', "Request $orderId began being processed by Mount Sinai Charcot on ${currentTime()}")

    calculateOrderSizeAndPartitionIntoBuckets(orderInfo)
    recordOrderSize(orderInfo, orderInfo.size)
    recordFileCount(orderInfo, orderInfo.fileNames.size())
    Map<Integer, List<String>> bucketToFileList = orderInfo.bucketToFileList
    // Capture original number of buckets before any filtering of already processed files takes place
    int totalZips = bucketToFileList.size()
    orderInfo.filesProcessed && filterAlreadyProcessedFiles(bucketToFileList, orderInfo.filesProcessed)

    /*
     * In reprocess scenarios, all files might have been processed already,
     * in which case bucketToFileList will be empty because filterAlreadyProcessedFiles() detected
     * that all files have already been processed.
     */
    int zipCnt = bucketToFileList ? bucketToFileList.keySet().min() + 1 : 0
    def canceled = bucketToFileList.find { Integer bucketNumber, List<String> filesToZip ->
      def startAll = System.currentTimeMillis()
      /*
       * Download the files to zip. The closure inside the if() returns true as soon as it detects
       * a cancel request.
       * Check order status frequently to see if cancel has been requested. We want to be
       * as timely as possible in honoring such requests to avoid wasteful processing
       */
      if (filesToZip.find { String fileName ->
          try {
            // Do not fail-fast if a file fails to download, just continue with the rest
            def startCurrent = System.currentTimeMillis()

            if (!fileName.endsWith('/')) {
              // If it doesn't end in '/', assumption is that there's a top level
              // file name ala .mrxs. Below we grab the folder component of this
              // image as well
              downloadS3Object(orderInfo, fileName)
            }

            // Check if cancel requested right before we commit to downloading
            // entire image folder
            if (cancelIfRequested(orderInfo)) {
              return true
            }
            downloadS3Object(orderInfo, fileName.replace('.mrxs', '/'))
            log.info "Took ${System.currentTimeMillis() - startCurrent} milliseconds to download $fileName for request $orderId"
          } catch (Exception e) {
            String msg = "Problem downloading $fileName"
            log.error msg, e
            updateOrderStatus(orderInfo, null, "$msg: ${e.toString()}")
          }
          false
        }) {
        log.info "Order $orderId canceled"
        return true
      }
      log.info "Took ${System.currentTimeMillis() - startAll} milliseconds to download all the image slides for request $orderId"

      // Create the manifest file
      createManifestFile(orderInfo, filesToZip)

      // Create zip
      String zipName = totalZips > 1 ? "$orderId-$zipCnt-of-${totalZips}.zip" : "${orderId}.zip"
      def startZip = System.currentTimeMillis()
      createZip(orderInfo, zipName)
      log.info "Took ${System.currentTimeMillis() - startZip} milliseconds to create zip for request $orderId"

      // Upload zip to S3
      def startUpload = System.currentTimeMillis()
      uploadObjectToS3(zipName)
      log.info "Took ${System.currentTimeMillis() - startUpload} milliseconds to upload zip for request $orderId"

      // Generate a signed URL
      String zipLink = generateSignedZipUrl(orderInfo, zipName)

      // Send email
      sendEmail(orderInfo, zipLink, zipCnt, totalZips)

      // cleanup in preparation for next batch, this way
      // we free up space so as to to avoid blowing disk space on the host
      cleanUp(orderInfo, zipName)

      ++zipCnt

      /*
       * Record the batch of processed files in order table. For now just record
       * the main .msxr file as representative of each of the sets of files in this bucket.
       */
      updateProcessedFiles(orderInfo, filesToZip)
      if (cancelIfRequested(orderInfo)) {
        return true
      }
      updateOrderStatus(orderInfo, 'processing', "${bucketNumber + 1} of $totalZips zip files sent to requester.")
      false
    }

    performOrderProcessedActions(orderInfo, !canceled)
  }

  Map<String, String> retrieveNextOrderId() {
    AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient()
    ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
      .withQueueUrl(sqsOrderQueueUrl)
      .withMaxNumberOfMessages(1)
    List<SQSMessage> messages = sqs.receiveMessage(receiveMessageRequest).getMessages()
    if (messages) {
      SQSMessage message = messages[0]
      return [orderId         : (new JsonSlurper().parseText(message.body.toString()) as Map<String, Object>).orderId as String,
        sqsReceiptHandle: message.receiptHandle]
    }
    null
  }

  void performOrderProcessedActions(OrderInfoDto orderInfo, boolean updateStatus = true) {
    AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient()
    sqs.deleteMessage(sqsOrderQueueUrl, orderInfo.sqsReceiptHandle)
    updateStatus && updateOrderStatus(orderInfo, 'processed', "Request processed successfully on ${currentTime()}")
  }

  void updateOrderStatus(OrderInfoDto orderInfo, String status, String remark = null) {
    status && updateOrder(orderInfo.orderId, orderInfo.recordNumber, [name: 'status', value: status, dataType: 's'] as AttributeUpdateInfo)
    updateOrderRemark(orderInfo, remark)
  }

  private void updateOrderRemark(OrderInfoDto orderInfo, String remark) {
    if (!remark) {
      return
    }
    String orderId = orderInfo.orderId
    Integer lastRecordNumber = orderInfo.lastRecordNumber
    OrderInfoDto orderInfoForLastRecord = orderInfo
    if (lastRecordNumber > 0) {
      orderInfoForLastRecord = retrieveOrderInfo(orderId, lastRecordNumber)
    }

    String delta = "[${currentTime()}] $remark"
    Pair<Integer, String> recordNumberAndUpdates = determineNextRecordNumberAndUpdates(orderId, lastRecordNumber, orderInfoForLastRecord.approximateItemSizeInBytes(), "$delta\n${orderInfoForLastRecord.remark ?: ''}", delta)
    updateOrder(orderId, recordNumberAndUpdates.left(), [name: 'remark', value: recordNumberAndUpdates.right(), dataType: 's'] as AttributeUpdateInfo)
  }

  void recordOrderSize(OrderInfoDto orderInfo, Long size) {
    updateOrder(orderInfo.orderId, orderInfo.recordNumber, [name: "size", value: size.toString(), dataType: "n"] as AttributeUpdateInfo)
  }

  void recordFileCount(OrderInfoDto orderInfo, Integer fileCount) {
    updateOrder(orderInfo.orderId, orderInfo.recordNumber, [name: "fileCount", value: fileCount.toString(), dataType: "n"] as AttributeUpdateInfo)
  }

  @CompileDynamic
  private void updateOrder(String orderId, Integer recordNumber, AttributeUpdateInfo update) {
    Map<String, String> expressionAttributeNames = [("#$update.name".toString()): update.name]
    Map<String, AttributeValue> expressionAttributeValues = [(":$update.name".toString()): (AttributeValue.builder()."$update.dataType"(update.value) as AttributeValue.Builder).build()]
    String updateExpression = "SET #$update.name = :$update.name"
    DynamoDbClient dynamoDB = DynamoDbClient.builder().build()
    UpdateItemResponse updateItemResponse = dynamoDB.updateItem(UpdateItemRequest.builder()
      .tableName(dynamoDbOrderTableName).updateExpression(updateExpression)
      .expressionAttributeNames(expressionAttributeNames)
      .expressionAttributeValues(expressionAttributeValues)
      .key([orderId: AttributeValue.builder().s(orderId).build(), recordNumber: AttributeValue.builder().n(recordNumber.toString()).build()]).build() as UpdateItemRequest)
    log.info "Updated request $orderId with $update, response was ${updateItemResponse.toString()}"
  }

  @ToString(includeNames = true, excludes = ['dataType'])
  private static class AttributeUpdateInfo {
    String name
    Object value
    String dataType
  }

  void updateSqsReceiptHandle(String orderId, Integer recordNumber, String sqsReceiptHandle) {
    updateOrder(orderId, recordNumber, new AttributeUpdateInfo(name: "sqsReceiptHandle", value: sqsReceiptHandle, dataType: "s"))
  }

  void updateProcessedFiles(OrderInfoDto orderInfo, List<String> files) {
    String orderId = orderInfo.orderId
    Integer lastRecordNumber = orderInfo.lastRecordNumber
    OrderInfoDto orderInfoForLastRecord = orderInfo
    if (lastRecordNumber > 0) {
      orderInfoForLastRecord = retrieveOrderInfo(orderId, lastRecordNumber)
    }

    /*
     * If the added files wouldn't fit in current record, create a new one.
     */
    Pair<Integer, String> recordNumberAndUpdates =
      determineNextRecordNumberAndUpdates(orderId, lastRecordNumber, orderInfoForLastRecord.approximateItemSizeInBytes(), (orderInfoForLastRecord.filesProcessed.toSet() + files.toSet()).join(','), (files.toSet() - orderInfoForLastRecord.filesProcessed.toSet()).join(','))
    updateOrder(orderId, recordNumberAndUpdates.left(), [name: "filesProcessed", value: (recordNumberAndUpdates.right().split(',')).collect { AttributeValue.builder().s(it).build() }, dataType: "l"] as AttributeUpdateInfo)
  }


  /**
   * Checks to see if the given combined updates wouldn't fit in the current DynamoDB item. If they do, then returns
   * the combined updates and current record number. Otherwise returns the additions (delta) and an incremented record number
   * to signal to caller that a new record in DynamoDB should be created and started with just the delta. Once/if that new record
   * fills up, the process repeats itself.
   *
   */
  private Pair<Integer, String> determineNextRecordNumberAndUpdates(String orderId, Integer currentRecordNumber, Integer currentOrderSize, String combinedUpdates, String delta) {
    Integer nextRecordNumber = currentRecordNumber
    Integer wouldBeOrderRecordSize = currentOrderSize + delta.getBytes('UTF-8').length
    String updates = combinedUpdates
    if ((currentOrderSize + updates.getBytes('UTF-8').length) > DYNAMODB_MAX_ITEM_SIZE_IN_BYTES) {
      ++nextRecordNumber
      updates = delta
      log.info "DynamoDB item limit of $DYNAMODB_MAX_ITEM_SIZE_IN_BYTES exceeded. Order $orderId would have size of $wouldBeOrderRecordSize, will store '$delta' to new recordNumber $nextRecordNumber"
    } else {
      log.info "DynamoDB item limit of $DYNAMODB_MAX_ITEM_SIZE_IN_BYTES NOT exceeded. Storing $updates to orderId $orderId and recordNumber $nextRecordNumber"
    }

    Pair.of(nextRecordNumber, updates)
  }

  OrderInfoDto retrieveOrderInfo(String orderId, Integer recordNumber = 0) {
    QueryRequest queryRequest = QueryRequest.builder()
      .keyConditionExpression('orderId=:orderId')
      .tableName(dynamoDbOrderTableName)
      .expressionAttributeValues([':orderId': AttributeValue.builder().s(orderId).build()])
      .build() as QueryRequest

    DynamoDbClient dynamoDB = DynamoDbClient.builder().build()
    List<Map<String, AttributeValue>> items = dynamoDB.query(queryRequest).items()

    if (!items) {
      return null
    }

    Map<String, AttributeValue> item = items.find {
      Integer.valueOf(it.recordNumber.n()) == recordNumber
    }

    OrderInfoDto orderInfoDto = new OrderInfoDto()
    orderInfoDto.orderId = orderId
    orderInfoDto.recordNumber = item.recordNumber.n().toInteger()
    orderInfoDto.fileNames = item.fileNames?.l()?.collect { it.s() }
    orderInfoDto.filesProcessed = item.filesProcessed?.l()?.collect { it.s() }
    orderInfoDto.filesProcessed = orderInfoDto.filesProcessed ?: []
    orderInfoDto.email = item.email?.s()
    orderInfoDto.filter = item.filter?.s()
    orderInfoDto.outputPath = "$WORK_FOLDER/$orderId"
    orderInfoDto.status = item.status?.s()
    orderInfoDto.remark = item.remark?.s()
    orderInfoDto.sqsReceiptHandle = item.sqsReceiptHandle?.s()
    orderInfoDto.lastRecordNumber = items.max {
      it.recordNumber.n().toInteger()
    }.recordNumber.n().toInteger()
    orderInfoDto
  }

  void downloadS3Object(OrderInfoDto orderInfoDto, String key) {
    log.info "Downloading $key..."
    AmazonS3 s3 = AmazonS3ClientBuilder.standard().build()
    List<String> keysToDownload = []
    if (key.endsWith('/')) {
      ObjectListing objectListing = s3.listObjects(s3OdpBucketName, key)
      keysToDownload += objectListing.objectSummaries.collect {
        it.key
      }
    } else {
      keysToDownload << key
    }
    performS3Operation({ S3TransferManager transferManager ->
      keysToDownload.each { String keyToDownload ->
        new File(Paths.get(orderInfoDto.outputPath, new File(keyToDownload).parent ?: "").toString()).mkdirs()
        FileDownload download =
          transferManager.downloadFile({ b ->
            b.destination(Paths.get(orderInfoDto.outputPath, keyToDownload)).getObjectRequest({ req ->
              req.bucket(s3OdpBucketName).key(keyToDownload)
            })
          })
        download.completionFuture().join()
      }
    })

    log.info "Download of $key complete"
  }

  void createManifestFile(OrderInfoDto orderInfoDto, List<String> fileNames) {
    String manifestFilePath = Paths.get(orderInfoDto.outputPath, 'manifest.csv').toString()
    log.info "Creating manifest file at $manifestFilePath"
    DynamoDbClient dynamoDB = DynamoDbClient.builder().build()
    File csvFile = new File(manifestFilePath)
    csvFile.parentFile.mkdirs()
    // Write out the header
    csvFile << (NUMBER_ATTRIBUTES + STRING_ATTRIBUTES).join(',') << "\n"
    fileNames.each { String fileName ->
      GetItemRequest request = GetItemRequest.builder()
        .key([fileName: AttributeValue.builder().s(fileName).build()])
        .tableName(dynamoDbImageMetadataTableName).build() as GetItemRequest
      Map<String, AttributeValue> items = dynamoDB.getItem(request).item()
      List<String> record = []
      NUMBER_ATTRIBUTES.each {
        record << items[it].n()
      }
      STRING_ATTRIBUTES.each {
        record << items[it].s()
      }
      csvFile << record.join(',') << "\n"
    }
    log.info "Done creating manifest file at $manifestFilePath"
  }

  private static void createZip(OrderInfoDto orderInfoDto, String zipName) {
    String orderId = orderInfoDto.orderId
    runCommand("zip -r -0 ${zipName - '.zip'} ./$orderId/".toString())
  }

  void uploadObjectToS3(String zipName) {
    systemStats()
    String zipPath = "$WORK_FOLDER/$zipName"
    log.info "Uploading Zip $zipPath to $s3ZipBucketName S3 bucket"
    def s3ClientBuilder = S3AsyncClient.crtBuilder()
      .minimumPartSizeInBytes(50000000)
    if (local) {
      s3ClientBuilder.credentialsProvider(ProfileCredentialsProvider.create(odpProfileName))
    }
    performS3Operation({ S3TransferManager transferManager ->
      FileUpload upload = transferManager.uploadFile({ UploadFileRequest.Builder b ->
        b.source(Paths.get(zipPath))
          .putObjectRequest({ req ->
            req.bucket(s3ZipBucketName).key(zipName)
          })
      })
      upload.completionFuture().join()
    })

    log.info "Uploaded Zip $zipPath to $s3ZipBucketName S3 bucket"
  }

  private void performS3Operation(Closure operation) {
    S3AsyncClient.crtBuilder().build().withCloseable { S3AsyncClient s3Client ->
      S3TransferManager.builder().s3Client(s3Client).build().withCloseable { S3TransferManager transferManager ->
        operation(transferManager)
      }
    }
  }

  String generateSignedZipUrl(OrderInfoDto orderInfoDto, String zipName) {
    AmazonS3 s3 = AmazonS3ClientBuilder.standard().build()

    /*
     * FIXME: Can't we just assume in local we always deploy everything to same account (mssm - paid account profile)? Doing
     *  Paid/ODP account stack split is the reason this was originally added, but do not think there's too much value in that split
     *  anymore.
     *  Update 05/27/2024: But Mt Sinai ODP account is the one where S3 storage is provided at no charge because it's for research purposes, maybe
     *    that's the reason I did the split.
     */
    if (local) {
      // In local the 'mssm-odp' AWS profile should exist
      s3 = AmazonS3ClientBuilder.standard().withCredentials(new ProfileCredentialsProviderV1(odpProfileName)).build()
    }

    String zipLink = s3.generatePresignedUrl(s3ZipBucketName, zipName, new DateTime().plusDays(7).toDate()).toExternalForm()
    log.info "Generated signed Zip link for request $orderInfoDto.orderId"
    zipLink
  }

  void sendEmail(OrderInfoDto orderInfoDto, String zipLink, int zipCnt, int totalZips) {
    AmazonSimpleEmailService client = AmazonSimpleEmailServiceClientBuilder.standard()
      .withRegion(Regions.US_EAST_1).build()
    SendEmailRequest request = new SendEmailRequest()
      .withDestination(new Destination().withToAddresses(orderInfoDto.email))
      .withMessage(new Message()
      .withBody(new Body()
      .withHtml(new Content().withCharset("UTF-8").withData("""\
                               Your requested image Zip is ready. You can access via this <a href='$zipLink'>link</a>
                              """.stripIndent()))
      .withText(new Content().withCharset("UTF-8").withData("""\
                                Your requested image Zip is ready. You can access via this link: ${zipLink}.
                              """.stripIndent())))
      .withSubject(new Content().withCharset("UTF-8")
      .withData("""\
                  Mount Sinai Charcot Image Request ($orderInfoDto.orderId) Ready${totalZips > 1 ? " for Batch $zipCnt of $totalZips" : ''
    }""".stripIndent()))).withSource(fromEmail)
  client.sendEmail(request)
  log.info "Sent email for request $orderInfoDto.orderId and zip link $zipLink"
}

private static void cleanUp(OrderInfoDto orderInfoDto, String zipName) {
  String targetFolder = orderInfoDto.outputPath
  String targetZip = "$WORK_FOLDER/$zipName"
  log.info "Cleaning up $targetFolder and $targetZip"
  FileUtils.deleteDirectory(new File(targetFolder))
  FileUtils.delete(new File(targetZip))
  systemStats()
}

private static void systemStats() {
  diskStats()
  memStats()
  fdStats()
}

private static void diskStats() {
  log.info "Disk Free Stats\n${'df -kh'.execute().text}"
}

private static void memStats() {
  //log.info "Memory Stats\n${'cat /proc/meminfo'.execute().text}"
}

private static void fdStats() {
  String fdStats = "ls -l /proc/${ProcessHandle.current().pid()}/fd".execute().text
  log.info "File Descriptor Stats:\n Total: ${(fdStats =~ /\d+ ->/).size()}\n$fdStats"
}

private static void runCommand(String command) {
  Process process = new ProcessBuilder(['sh', '-c', command])
  .directory(new File(WORK_FOLDER))
  .redirectErrorStream(true)
  .start()

  def outputStream = new OutputStream() {
      @Override
      void write(final int b) throws IOException {
        // NOOP
      }

      @Override
      void write(byte[] buf, int off, int len) throws IOException {
        log.info "${new String(buf[off..len - 1].toArray() as byte[], Charset.defaultCharset())}"
      }
    }
  // Start getting output right away rather than wait for command to finish
  process.consumeProcessOutput(outputStream, outputStream)
  process.waitFor()

  if (process.exitValue()) {
    log.info "Problem running command $command: $process.err.text"
  } else {
    log.info "Successfully ran command $command"
  }
}

/**
 * Creates buckets numbered 0 through N, where each buckets contains a maximum of FILE_BUCKET_SIZE. The reason
 * for this is to make deterministic the size of each Zip generated.
 * It also calculates total order size in bytes. All of this info is stored in the passed in
 * order info DTO object.*/
void calculateOrderSizeAndPartitionIntoBuckets(OrderInfoDto orderInfoDto) {
  log.info "Partitioning file list into buckets up to size $FILE_BUCKET_SIZE"
  AmazonS3 s3 = AmazonS3ClientBuilder.standard().build()
  Integer bucketNum = 0
  Long cumulativeObjectsSize = 0
  orderInfoDto.bucketToFileList = orderInfoDto.fileNames.inject([:] as Map<Integer, List<String>>) { Map<Integer, List<String>> bucketToImages, String file ->
    // FIXME: Are we missing the .mrxs file size?
    ObjectListing objectListing = s3.listObjects(s3OdpBucketName, file.replace('.mrxs', '/'))
    cumulativeObjectsSize = objectListing.objectSummaries.inject(cumulativeObjectsSize) { Long size, S3ObjectSummary objectSummary ->
      size + s3.getObjectMetadata(s3OdpBucketName, objectSummary.key).contentLength
    }

    orderInfoDto.size += cumulativeObjectsSize

    // If the current file caused the size to go over the limit per bucket,
    // time to start a new bucket
    if (cumulativeObjectsSize > FILE_BUCKET_SIZE) {
      log.info "Bucket $bucketNum full, it contains ${bucketToImages[bucketNum].size()} files"
      ++bucketNum
      bucketToImages << [(bucketNum): [file]]
      log.info "Starting new bucket $bucketNum with $file because $cumulativeObjectsSize exceeds $FILE_BUCKET_SIZE"
      cumulativeObjectsSize = 0
    } else {
      bucketToImages.get(bucketNum, []) << file
      log.info "Added $file to bucket $bucketNum, size thus far is $cumulativeObjectsSize"
    }

    bucketToImages
  }
}

/**
 * This method exists to support resume/reprocess fulfillment scenario where for example there was an unexpected error
 * and now we manually resume this request/order form where it left off, to avoid sending duplicate Zip's
 * to the requester.*/
private static void filterAlreadyProcessedFiles(Map<Integer, List<String>> bucketToImages, List<String> alreadyProcessedFiles) {
  Map<Integer, List<String>> newMap = bucketToImages.collectEntries { Integer bucket, List<String> files ->
    // See if this bucket's file have all been processed
    if (!(files - alreadyProcessedFiles)) {
      log.info("Removing bucket $bucket because all the files there were already processed.")
      return [:]
    }
    [(bucket): files]
  }
  bucketToImages.clear()
  bucketToImages.putAll(newMap)
}
}
