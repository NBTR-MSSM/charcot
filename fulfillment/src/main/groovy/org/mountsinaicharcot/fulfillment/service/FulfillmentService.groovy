package org.mountsinaicharcot.fulfillment.service

import com.amazonaws.auth.profile.ProfileCredentialsProvider as ProfileCredentialsProviderV1
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate
import com.amazonaws.services.dynamodbv2.model.GetItemRequest
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult
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
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
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
import software.amazon.awssdk.transfer.s3.FileDownload
import software.amazon.awssdk.transfer.s3.FileUpload
import software.amazon.awssdk.transfer.s3.S3ClientConfiguration
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.UploadFileRequest

import java.nio.charset.Charset
import java.nio.file.Paths

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

  final static String workFolder = './.charcot'

  final static Long FILE_BUCKET_SIZE = 50000000000

  final static List<String> numberAttributes = ['subjectNumber', 'age']

  final static List<String> stringAttributes = ['race', 'diagnosis', 'sex', 'region', 'stain', 'fileName']

  /**
   * After Spring application context starts up, set up an infinite loop of polling SQS for new messages
   */
  void run(String... args) throws Exception {
    log.info "Entering queue poll loop"
    while (true) {
      Map<String, String> orderInfoFromSqs
      try {
        orderInfoFromSqs = retrieveNextOrderId()
        if (!orderInfoFromSqs) {
          continue
        }
        def orderInfoDto = retrieveOrderInfo(orderInfoFromSqs.orderId)
        if (orderInfoDto.status != 'received') {
          /*
           * Another worker already processed or processing this order. If the request is large,
           * the AWS SQS max visibility timeout window of 12 hours can/will be exhausted and another worker will see
           * the message again, which would result in duplicate work on this order. Our escape hatch for
           * that is to rely on order status to know whenever the worker is done processing the order. Also this fetch has
           * the effect of extending the visibility timeout by another 12 hours, which is a good thing.
           */
          continue
        }
        fulfill(orderInfoDto, orderInfoFromSqs.sqsReceiptHandle)
      } catch (Exception e) {
        log.error "Problem fulfilling $orderInfoFromSqs.orderId", e
        updateOrderStatus(orderInfoFromSqs.orderId, 'failed', e.toString())
      }
    }
  }

  boolean cancelIfRequested(String orderId) {
    def orderInfoDto = retrieveOrderInfo(orderId)
    if (orderInfoDto.status == 'cancel-requested') {
      updateOrderStatus(orderId, 'canceled')
      return true
    }
    false
  }

  private String currentTime() {
    DateTimeZone utc = DateTimeZone.forID('GMT')
    DateTime dt = new DateTime(utc)
    println dt
    DateTimeFormatter fmt = DateTimeFormat.forPattern('E, d MMM, yyyy HH:mm:ssz')
    StringBuilder now = new StringBuilder()
    fmt.printTo(now, dt)
    now.toString()
  }

  void fulfill(OrderInfoDto orderInfoDto, String sqsReceiptHandle = null) {
    systemStats()
    String orderId = orderInfoDto.orderId
    log.info "Fulfilling order ${orderInfoDto.toString()}"
    String processingMsg = "Request $orderId began being processed by Mount Sinai Charcot on ${currentTime()}"
    updateOrderStatus(orderId, 'processing', processingMsg)

    List<String> fileNames = orderInfoDto.fileNames
    Map<Integer, List<String>> bucketToFileList = partitionFileListIntoBucketsUpToSize(fileNames)
    // Report on original number of buckets before any filtering of processed files takes place
    int totalZips = bucketToFileList.size()
    orderInfoDto.filesProcessed && filterAlreadyProcessedFiles(bucketToFileList, orderInfoDto.filesProcessed)
    int zipCnt = bucketToFileList.keySet().min() + 1
    def canceled = bucketToFileList.find { Integer bucketNumber, List<String> filesToZip ->
      def startAll = System.currentTimeMillis()
      /*
       * Download the files to zip. The closure inside the if() returns true as soon as it detects
       * a cancel request.
       * Check order status frequently to see if cancel has been requested. We want to be
       * as timely as possible in honoring such requests to avoid wasteful processing
       */
      if (filesToZip.find { String fileName ->
        def startCurrent = System.currentTimeMillis()
        downloadS3Object(orderInfoDto, fileName)
        // Check if cancel requested right before we commit to downloading
        // entire .mrxs image folder
        if (cancelIfRequested(orderId)) {
          return true
        }
        downloadS3Object(orderInfoDto, fileName.replace('.mrxs', '/'))
        log.info "Took ${System.currentTimeMillis() - startCurrent} milliseconds to download $fileName for request $orderId"
        false
      }) {
        log.info "Order $orderId canceled"
        return true
      }
      log.info "Took ${System.currentTimeMillis() - startAll} milliseconds to download all the image slides for request $orderId"

      // Create the manifest file
      createManifestFile(orderInfoDto, filesToZip)

      // Create zip
      String zipName = totalZips > 1 ? "$orderId-$zipCnt-of-${totalZips}.zip" : "${orderId}.zip"
      def startZip = System.currentTimeMillis()
      createZip(orderInfoDto, zipName)
      log.info "Took ${System.currentTimeMillis() - startZip} milliseconds to create zip for request $orderId"

      // Upload zip to S3
      def startUpload = System.currentTimeMillis()
      uploadObjectToS3(zipName)
      log.info "Took ${System.currentTimeMillis() - startUpload} milliseconds to upload zip for request $orderId"

      // Generate a signed URL
      String zipLink = generateSignedZipUrl(orderInfoDto, zipName)

      // Send email
      sendEmail(orderInfoDto, zipLink, zipCnt, totalZips)

      // cleanup in preparation for next batch, this way
      // we free up space so as to to avoid blowing disk space on the host
      cleanUp(orderInfoDto, zipName)

      ++zipCnt

      /*
     * Record the batch of processed files in order table. For now just record
     * the main .msxr file as representative of each of the sets that are part of this bucket.
     */
      updateProcessedFiles(orderId, filesToZip)
      if (cancelIfRequested(orderId)) {
        return true
      }
      updateOrderStatus(orderId, 'processing', "$processingMsg, ${bucketNumber + 1} of $totalZips zip files sent to requester.")
      false
    }

    markOrderAsProcessed(orderId, sqsReceiptHandle, !canceled)
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
    return null
  }

  void markOrderAsProcessed(String orderId, String sqsReceiptHandle, boolean updateStatus = true) {
    AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient()
    sqs.deleteMessage(sqsOrderQueueUrl, sqsReceiptHandle)
    updateStatus && updateOrderStatus(orderId, 'processed', "Request processed successfully on ${currentTime()}")
  }

  void updateOrderStatus(String orderId, String status, String remark = null) {
    AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient()
    Map<String, AttributeValueUpdate> attributeUpdates = [:]
    status && attributeUpdates.put('status', new AttributeValueUpdate().withValue(new AttributeValue().withS(status)))
    remark && attributeUpdates.put('remark', new AttributeValueUpdate().withValue(new AttributeValue().withS(remark)))
    UpdateItemResult updateItemResult = dynamoDB.updateItem(dynamoDbOrderTableName,
            [orderId: new AttributeValue().withS(orderId)], attributeUpdates)
    log.info "Update request $orderId status to $status, ${updateItemResult.toString()}"
  }

  void updateProcessedFiles(String orderId, List<String> files) {
    OrderInfoDto orderInfoDto = retrieveOrderInfo(orderId)
    // Merge current files processed with new ones, creating a set of unique values, and store
    // back into DB, overriding existing list of files processed
    Set<String> filesProcessed = orderInfoDto.filesProcessed.toSet() + files.toSet()
    AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient()
    UpdateItemResult updateItemResult = dynamoDB.updateItem(dynamoDbOrderTableName,
            [orderId: new AttributeValue().withS(orderId)],
            // FIXME: Is this replacing instead of adding to the list of already processed files???
            // No need: [REF|https://groovy-lang.org/objectorientation.html#_varargs|"If a varargs method is called with an array as an argument, then the argument will be that array instead of an array of length one containing the given array as the only element."]
            [filesProcessed: new AttributeValueUpdate().withValue(new AttributeValue().withL(filesProcessed.collect { new AttributeValue().withS(it) }))])
    log.info "Update request $orderId processed files to $files, ${updateItemResult.toString()}"
  }

  OrderInfoDto retrieveOrderInfo(String orderId) {
    GetItemRequest request = new GetItemRequest()
            .withKey([orderId: new AttributeValue(orderId)])
            .withTableName(dynamoDbOrderTableName)

    AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient()
    Map<String, AttributeValue> item = dynamoDB.getItem(request).getItem()

    if (!item) {
      return null
    }

    OrderInfoDto orderInfoDto = new OrderInfoDto()
    orderInfoDto.fileNames = item.fileNames.l.collect { it.s }
    orderInfoDto.filesProcessed = item.filesProcessed?.l?.collect { it.s }
    orderInfoDto.filesProcessed = orderInfoDto.filesProcessed ?: []
    orderInfoDto.email = item.email.s
    orderInfoDto.orderId = orderId
    orderInfoDto.outputPath = "$workFolder/$orderId"
    orderInfoDto.status = item.status.s
    orderInfoDto
  }

  void downloadS3Object(OrderInfoDto orderInfoDto, String key) {
    log.info "Downloading $key..."
    AmazonS3 s3 = AmazonS3ClientBuilder.standard().build()
    List<String> keysToDownload = []
    if (key.endsWith('/')) {
      new File(Paths.get(orderInfoDto.outputPath, key).toString()).mkdirs()
      ObjectListing objectListing = s3.listObjects(s3OdpBucketName, key)
      keysToDownload += objectListing.objectSummaries.collect {
        it.key
      }
    } else {
      new File(Paths.get(orderInfoDto.outputPath).toString()).mkdirs()
      keysToDownload << key
    }

    try (S3TransferManager transferManager = S3TransferManager.create()) {
      keysToDownload.each { String keyToDownload ->
        FileDownload download =
                transferManager.downloadFile({ b ->
                  b.destination(Paths.get(orderInfoDto.outputPath, keyToDownload)).getObjectRequest({ req -> req.bucket(s3OdpBucketName).key(keyToDownload)
                  })
                })
        download.completionFuture().join()
      }
    }
    log.info "Download of $key complete"
  }

  void createManifestFile(OrderInfoDto orderInfoDto, List<String> fileNames) {
    String manifestFilePath = Paths.get(orderInfoDto.outputPath, 'manifest.csv').toString()
    log.info "Creating manifest file at $manifestFilePath"
    AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient()
    File csvFile = new File(manifestFilePath)
    csvFile.parentFile.mkdirs()
    // Write out the header
    csvFile << (numberAttributes + stringAttributes).join(',') << "\n"
    fileNames.each { String fileName ->
      GetItemRequest request = new GetItemRequest()
              .withKey([fileName: new AttributeValue(fileName)])
              .withTableName(dynamoDbImageMetadataTableName)
      Map<String, AttributeValue> items = dynamoDB.getItem(request).getItem()
      List<String> record = []
      numberAttributes.each {
        record << items[it].n
      }
      stringAttributes.each {
        record << items[it].s
      }
      csvFile << record.join(',') << "\n"
    }
    log.info "Done creating manifest file at $manifestFilePath"
  }

  void createZip(OrderInfoDto orderInfoDto, String zipName) {
    String orderId = orderInfoDto.orderId
    runCommand("zip -r -0 ${zipName - '.zip'} ./$orderId/".toString())
  }

  void uploadObjectToS3(String zipName) {
    systemStats()
    String zipPath = "$workFolder/$zipName"
    log.info "Uploading Zip $zipPath to $s3ZipBucketName S3 bucket"
    try (S3TransferManager transferManager = S3TransferManager.builder().s3ClientConfiguration({ S3ClientConfiguration.Builder cfg ->
      cfg.minimumPartSizeInBytes(50000000)
      if (local) {
        cfg.credentialsProvider(ProfileCredentialsProvider.create(odpProfileName))
      }
    }).build()) {
      FileUpload upload = transferManager.uploadFile({ UploadFileRequest.Builder b ->
        b.source(Paths.get(zipPath))
                .putObjectRequest({ req -> req.bucket(s3ZipBucketName).key(zipName)
                })
      })
      upload.completionFuture().join()
    }
    log.info "Uploaded Zip $zipPath to $s3ZipBucketName S3 bucket"
  }

  String generateSignedZipUrl(OrderInfoDto orderInfoDto, String zipName) {
    AmazonS3 s3 = AmazonS3ClientBuilder.standard().build()

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
            .withDestination(
                    new Destination().withToAddresses(orderInfoDto.email))
            .withMessage(new Message()
                    .withBody(new Body()
                            .withHtml(new Content().withCharset("UTF-8").withData("""\
                               Your requested image Zip is ready. You can access via this <a href='$zipLink'>link</a>
                              """.stripIndent()))
                            .withText(new Content().withCharset("UTF-8").withData("""\
                                Your requested image Zip is ready. You can access via this link: ${zipLink}.
                              """.stripIndent())))
                    .withSubject(new Content().withCharset("UTF-8")
                            .withData("Mount Sinai Charcot Image Request ($orderInfoDto.orderId) Ready${totalZips > 1 ? " for Batch $zipCnt of $totalZips" : ''}"))).withSource(fromEmail)
    client.sendEmail(request)
    log.info "Sent email for request $orderInfoDto.orderId and zip link $zipLink"
  }

  void cleanUp(OrderInfoDto orderInfoDto, String zipName) {
    String targetFolder = orderInfoDto.outputPath
    String targetZip = "$workFolder/$zipName"
    log.info "Cleaning up $targetFolder and $targetZip"
    FileUtils.deleteDirectory(new File(targetFolder))
    FileUtils.delete(new File(targetZip))
    systemStats()
  }

  void systemStats() {
    diskStats()
    memStats()
    fdStats()
  }

  void diskStats() {
    log.info "Disk Free Stats\n${'df -kh'.execute().text}"
  }

  void memStats() {
    log.info "Memory Stats\n${'cat /proc/meminfo'.execute().text}"
  }

  void fdStats() {
    String fdStats = "ls -l /proc/${ProcessHandle.current().pid()}/fd".execute().text
    log.info "File Descriptor Stats:\n Total: ${(fdStats =~ /\d+ ->/).size()}\n$fdStats"
  }

  private void runCommand(String command) {
    Process process = new ProcessBuilder(['sh', '-c', command])
            .directory(new File(workFolder))
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
    // Start getting output right away rather than way for command to finish
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
   * for this is that we make it deterministic the size of each Zip generated.
   */
  Map<Integer, List<String>> partitionFileListIntoBucketsUpToSize(List<String> files) {
    log.info "Partitioning file list into buckets up to size $FILE_BUCKET_SIZE"
    AmazonS3 s3 = AmazonS3ClientBuilder.standard().build()
    Integer bucketNum = 0
    Long cumulativeObjectsSize = 0
    files.inject([:] as Map<Integer, List<String>>) { Map<Integer, List<String>> bucketToImages, String file ->
      ObjectListing objectListing = s3.listObjects(s3OdpBucketName, file.replace('.mrxs', '/'))
      cumulativeObjectsSize = objectListing.objectSummaries.inject(cumulativeObjectsSize) { Long size, S3ObjectSummary objectSummary ->
        size + s3.getObjectMetadata(s3OdpBucketName, objectSummary.key).contentLength
      }

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
        log.info "Added $file to bucket $bucketNum, size thus far us $cumulativeObjectsSize"
      }

      bucketToImages
    }
  }

  /**
   * This method exists to support resume fulfillment scenario where for example there was an unexpected error
   * and now we manually resume this request/order form where it left off, to avoid sending duplicate Zip's
   * to the requester.
   */
  private void filterAlreadyProcessedFiles(Map<Integer, List<String>> bucketToImages, List<String> alreadyProcessedFiles) {
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
