package org.mountsinaicharcot.fulfillment.service

import com.amazonaws.auth.profile.ProfileCredentialsProvider as ProfileCredentialsProviderV1
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.GetItemRequest
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
import groovy.util.logging.Slf4j
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime
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
  String activeProfile;

  final static String workFolder = '/tmp'

  final static Long FILE_BUCKET_SIZE = 50000000000

  final static List<String> numberAttributes = ['subjectNumber', 'age']

  final static List<String> stringAttributes = ['race', 'diagnosis', 'sex', 'region', 'stain', 'fileName']

  /**
   * After Spring application context starts up, set up an infinite loop of polling SQS for new messages
   */
  void run(String... args) throws Exception {
    log.info "Entering queue poll loop"
    while (true) {
      Map<String, String> orderInfo
      try {
        orderInfo = retrieveNextOrderId()
        if (!orderInfo) {
          continue
        }
        fulfill(retrieveOrderInfo(orderInfo.orderId), orderInfo.sqsReceiptHandle)
      } catch (Exception e) {
        log.error "Problem fulfilling $orderInfo.orderId", e
      }
    }
  }

  void fulfill(OrderInfoDto orderInfoDto, String sqsReceiptHandle = null) {
    systemStats()
    String orderId = orderInfoDto.orderId
    log.info "Fulfilling order $orderId"

    List<String> fileNames = orderInfoDto.fileNames
    int zipCnt = 1
    Map<Integer, List<String>> bucketToFileList = partitionFileListIntoBucketsUpToSize(fileNames)
    int totalZips = bucketToFileList.size()
    bucketToFileList.each { Integer bucketNumber, List<String> filesToZip ->
      def startAll = System.currentTimeMillis()

      // Download the files to zip
      filesToZip.each { String fileName ->
        def startCurrent = System.currentTimeMillis()
        downloadS3Object(orderInfoDto, fileName)
        downloadS3Object(orderInfoDto, fileName.replace('.mrxs', '/'))
        log.info "Took ${System.currentTimeMillis() - startCurrent} milliseconds to download $fileName for request $orderId"
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
    }
    markOrderAsProcessed(sqsReceiptHandle)
  }

  Map<String, String> retrieveNextOrderId() {
    AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient()
    ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
            .withQueueUrl(sqsOrderQueueUrl)
            .withMaxNumberOfMessages(1).withWaitTimeSeconds()
    List<SQSMessage> messages = sqs.receiveMessage(receiveMessageRequest).getMessages()
    if (messages) {
      SQSMessage message = messages[0]
      return [orderId         : (new JsonSlurper().parseText(message.body.toString()) as Map<String, Object>).orderId,
              sqsReceiptHandle: message.receiptHandle]
    }
    return null
  }

  void markOrderAsProcessed(String sqsReceiptHandle) {
    AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient()
    sqs.deleteMessage(sqsOrderQueueUrl, sqsReceiptHandle)
  }

  OrderInfoDto retrieveOrderInfo(String orderId) {
    GetItemRequest request = new GetItemRequest()
            .withKey([orderId: new AttributeValue(orderId)])
            .withTableName(dynamoDbOrderTableName)

    AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient()
    Map<String, AttributeValue> items = dynamoDB.getItem(request).getItem()

    if (!items) {
      return null
    }

    OrderInfoDto orderInfoDto = new OrderInfoDto()
    orderInfoDto.fileNames = items.fileNames.l.collect { AttributeValue fileNameAttribute -> fileNameAttribute.s }
    orderInfoDto.email = items.email.s
    orderInfoDto.orderId = orderId
    orderInfoDto.outputPath = "$workFolder/$orderId"
    orderInfoDto
  }

  void downloadS3Object(OrderInfoDto orderInfoDto, String key) {
    log.info "Downloading $key..."
    String orderId = orderInfoDto.orderId
    AmazonS3 s3 = AmazonS3ClientBuilder.standard().build()
    List<String> keysToDownload = []
    if (key.endsWith('/')) {
      new File(Paths.get(orderInfoDto.outputPath, key).toString()).mkdirs()
      ObjectListing objectListing = s3.listObjects(s3OdpBucketName, key)
      keysToDownload += objectListing.objectSummaries.collect {
        it.key
      }
    } else {
      new File(Paths.get(orderInfoDto.outputPath).toString()).mkdir()
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
                            .withData("Mount Sinai Charcot Image Request Ready${totalZips > 1 ? " for Batch $zipCnt of $totalZips" : ''}"))).withSource(fromEmail)
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
    // runCommand($/vm_stat | perl -ne '/page size of (\d+)/ and $$size=$1; /Pages\s+([^:]+)[^\d]+(\d+)/ and printf("%-16s % 16.2f Mi\n", "$1:", $2 * $$size / 1048576);'/$)
    log.info "Memory Stats\n${'cat /proc/meminfo'.execute().text}"
  }

  void fdStats() {
    log.info "File Descriptor Stats: ${("ls -l /proc/${ProcessHandle.current().pid()}/fd".execute().text =~ /\d+ ->/).size()}"
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

  Map<Integer, List<String>> partitionFileListIntoBucketsUpToSize(List<String> files) {
    log.info "Partitioning file list into buckets up to size $FILE_BUCKET_SIZE"
    AmazonS3 s3 = AmazonS3ClientBuilder.standard().build()
    Integer bucketNum = 0
    Long cumulativeObjectsSize = 0
    files.inject([:]) { Map<Integer, List<String>> bucketToImages, String file ->
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
        log.info "Added $file to bucket $bucketNum, size thus far us $cumulativeObjectsSize"
        bucketToImages.get(bucketNum, []) << file
      }

      bucketToImages
    }
  }
}
