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
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import java.nio.charset.Charset
import java.nio.file.Paths
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime
import org.mountsinaicharcot.fulfillment.dto.OrderInfoDto
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.stereotype.Service
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.FileDownload
import software.amazon.awssdk.transfer.s3.model.FileUpload
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest

@Service
@Slf4j
@CompileStatic
class FulfillmentService implements CommandLineRunner {
  private static final Long FILE_BUCKET_SIZE = 50000000000

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

  @Autowired
  OrderService orderService

  private static final List<String> NUMBER_ATTRIBUTES = ['subjectNumber', 'age']

  private static final List<String> STRING_ATTRIBUTES = [
    'race',
    'diagnosis',
    'sex',
    'region',
    'stain',
    'fileName'
  ]

  /**
   * After Spring application context starts up, set up an infinite loop of polling SQS for new messages*/
  void run(String... args) throws Exception {
    log.info "Entering queue poll loop"
    while (true) {
      Map<String, String> orderInfoFromSqs
      OrderInfoDto orderInfo
      try {
        orderInfoFromSqs = orderService.retrieveNextOrderId()
        if (!orderInfoFromSqs) {
          continue
        }
        orderInfo = orderService.retrieveOrderInfo(orderInfoFromSqs.orderId)
        orderInfo.sqsReceiptHandle = orderInfoFromSqs.sqsReceiptHandle
        orderService.updateSqsReceiptHandle(orderInfo.orderId, orderInfo.recordNumber, orderInfo.sqsReceiptHandle)
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
        orderService.failOrder(orderInfo.orderId, e)
      }
    }
  }

  void fulfill(OrderInfoDto orderInfo) {
    systemStats()
    String orderId = orderInfo.orderId
    log.info "Fulfilling order ${orderInfo.toString()}"
    orderService.startOrder(orderId)

    def canceled = null
    if (orderInfo.filesProcessed.size() != orderInfo.fileNames.size()) {
      calculateOrderSizeAndPartitionIntoBuckets(orderInfo)
      orderService.recordOrderSize(orderInfo.orderId, orderInfo.size)
      orderService.recordFileCount(orderInfo.orderId, orderInfo.fileNames.size())
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
      canceled = bucketToFileList.find { Integer bucketNumber, List<String> filesToZip ->
        def startAll = System.currentTimeMillis()
        /*
         * Download the files to zip. The closure inside the if() returns true as soon as it detects
         * a cancel request.
         * Check order status frequently to see if cancel has been requested. We want to be
         * as timely as possible in honoring such requests to avoid wasteful processing
         */
        if (filesToZip.find { String fileName ->
            try {
              def startCurrent = System.currentTimeMillis()

              if (!fileName.endsWith('/')) {
                // If it doesn't end in '/', assumption is that there's a top level
                // file name ala .mrxs. Farther below we grab the folder component of this
                // image as well
                downloadS3Object(orderInfo.outputPath, fileName)
              }

              // Check if cancel requested right before we commit to downloading
              // entire image folder
              if (orderService.cancelIfRequested(orderInfo.orderId)) {
                return true
              }
              downloadS3Object(orderInfo.outputPath, fileName.replace('.mrxs', '/'))
              log.info "Took ${System.currentTimeMillis() - startCurrent} milliseconds to download $fileName for request $orderId"
            } catch (Exception e) {
              // Do not fail-fast if a file fails to download, just continue with the rest
              String msg = "Problem downloading $fileName"
              log.error msg, e
              orderService.updateOrderStatus(orderInfo.orderId, null, "$msg: ${e.toString()}")
            }
            false
          }) {
          log.info "Order $orderId canceled"
          return true
        }
        log.info "Took ${System.currentTimeMillis() - startAll} milliseconds to download all the image slides for request $orderId"

        // Create the manifest file
        createManifestFile(orderInfo.outputPath, filesToZip)

        // Create zip
        String zipName = totalZips > 1 ? "$orderId-$zipCnt-of-${totalZips}.zip" : "${orderId}.zip"
        def startZip = System.currentTimeMillis()
        createZip(orderInfo.orderId, zipName)
        log.info "Took ${System.currentTimeMillis() - startZip} milliseconds to create zip for request $orderId"

        // Upload zip to S3
        def startUpload = System.currentTimeMillis()
        uploadObjectToS3(zipName)
        log.info "Took ${System.currentTimeMillis() - startUpload} milliseconds to upload zip for request $orderId"

        // Generate a signed URL
        String zipLink = generateSignedZipUrl(orderInfo.orderId, zipName)

        // Send email
        sendEmail(orderInfo.orderId, orderInfo.email, zipLink, zipCnt, totalZips)

        // cleanup in preparation for next batch, this way
        // we free up space so as to to avoid blowing disk space on the host
        cleanUp(orderInfo.outputPath, zipName)

        ++zipCnt

        /*
         * Record the batch of processed files in order table. For now just record
         * the main .msxr file as representative of each of the sets of files in this bucket.
         */
        orderService.updateFilesProcessed(orderInfo.orderId, filesToZip)
        if (orderService.cancelIfRequested(orderInfo.orderId)) {
          return true
        }
        orderService.updateOrderStatus(orderInfo.orderId, 'processing', "${bucketNumber + 1} of $totalZips zip files sent to requester.")
        false
      }
    } else {
      log.info "Looks like all $orderInfo.fileNames of $orderInfo.orderId were already processed"
    }

    orderService.performOrderProcessedActions(orderInfo.orderId, !canceled)
  }

  void downloadS3Object(String outputPath, String key) {
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
        new File(Paths.get(outputPath, new File(keyToDownload).parent ?: "").toString()).mkdirs()
        FileDownload download =
          transferManager.downloadFile({ b ->
            b.destination(Paths.get(outputPath, keyToDownload)).getObjectRequest({ req ->
              req.bucket(s3OdpBucketName).key(keyToDownload)
            })
          })
        download.completionFuture().join()
      }
    })

    log.info "Download of $key complete"
  }

  void createManifestFile(String outputPath, List<String> fileNames) {
    String manifestFilePath = Paths.get(outputPath, 'manifest.csv').toString()
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

  private static void createZip(String orderId, String zipName) {
    runCommand("zip -r -0 ${zipName - '.zip'} ./$orderId/".toString())
  }

  void uploadObjectToS3(String zipName) {
    systemStats()
    String zipPath = "$FulfillmentUtil.WORK_FOLDER/$zipName"
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

  String generateSignedZipUrl(String orderId, String zipName) {
    AmazonS3 s3 = AmazonS3ClientBuilder.standard().build()

    /*
     * FIXME: Can't we just assume in local we always deploy everything to same account (mssm - paid account profile)? Doing
     *  Paid/ODP account stack split is the reason this was originally added, but do not think there's too much value in that split
     *  anymore.
     *  Update 09/27/2024: Once I change [REF|/Users/jmquij0106/git/charcot/package.json|'"deploy:debug": "./script/deploy.mjs deploy -p mssm -o mssm -s debug"']
     *   to use the proper 'mssm-odp' profile for deployment, this logic will be mor necessary than ever. I want to use ODP for Zip bucket because
     *   it doesn't cost Mt Sinai $$$. Recall that S3 can get costly if we were to use the Mt Sinai paid account
     *  Update 05/27/2024: But Mt Sinai ODP account is the one where S3 storage is provided at no charge because it's for research purposes, maybe
     *    that's the reason I did the split?
     */
    if (local) {
      // In local the 'mssm-odp' AWS profile should exist, and should have full peermission to read from Zip bucket
      s3 = AmazonS3ClientBuilder.standard().withCredentials(new ProfileCredentialsProviderV1(odpProfileName)).build()
    }

    String zipLink = s3.generatePresignedUrl(s3ZipBucketName, zipName, new DateTime().plusDays(7).toDate()).toExternalForm()
    log.info "Generated signed Zip link for request $orderId"
    zipLink
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

  void sendEmail(String orderId, String email, String zipLink, int zipCnt, int totalZips) {
    String progress = totalZips > 1 ? " for Batch $zipCnt of $totalZips" : ''
    AmazonSimpleEmailService client = AmazonSimpleEmailServiceClientBuilder.standard()
      .withRegion(Regions.US_EAST_1).build()
    SendEmailRequest request = new SendEmailRequest()
      .withDestination(new Destination().withToAddresses(email))
      .withMessage(new Message()
      .withBody(new Body()
      .withHtml(new Content().withCharset("UTF-8")
      .withData("Your requested image Zip is ready. You can access via this <a href='$zipLink'>link</a>"))
      .withText(new Content().withCharset("UTF-8")
      .withData("Your requested image Zip is ready. You can access via this link: ${zipLink}.")))
      .withSubject(new Content().withCharset("UTF-8")
      .withData("Mount Sinai Charcot Image Request ($orderId) Ready$progress")))
      .withSource(fromEmail)
    client.sendEmail(request)
    log.info "Sent email for request $orderId and zip link $zipLink"
  }

  private static void cleanUp(String outputPath, String zipName) {
    String targetZip = "$FulfillmentUtil.WORK_FOLDER/$zipName"
    log.info "Cleaning up $outputPath and $targetZip"
    FileUtils.deleteDirectory(new File(outputPath))
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
    .directory(new File(FulfillmentUtil.WORK_FOLDER))
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
