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
import groovy.util.logging.Slf4j
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime
import org.mountsinaicharcot.fulfillment.dto.OrderInfoDto
import org.springframework.beans.factory.annotation.Value
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
class FulfillmentService {
  @Value('${charcot.dynamodb.order.table.name}')
  String dynamoDbOrderTableName

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

  final static String workFolder = '/root'

  final static Long FILE_BUCKET_SIZE = 50000000000

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
    orderInfoDto.fileNames = items.fileNames.l.collect { AttributeValue fileNameAttribute -> fileNameAttribute.s
    }
    orderInfoDto.email = items.email.s
    orderInfoDto.orderId = orderId
    orderInfoDto
  }

  void downloadS3Object(String orderId, String key) {
    log.info "Downloading $key..."
    AmazonS3 s3 = AmazonS3ClientBuilder.standard().build()
    List<String> keysToDownload = []
    if (key.endsWith('/')) {
      new File(Paths.get("$workFolder/", orderId, key).toString()).mkdirs()
      ObjectListing objectListing = s3.listObjects(s3OdpBucketName, key)
      keysToDownload += objectListing.objectSummaries.collect {
        it.key
      }
    } else {
      new File(Paths.get(workFolder, orderId).toString()).mkdir()
      keysToDownload << key
    }

    S3TransferManager transferManager = S3TransferManager.create()
    keysToDownload.each { String keyToDownload ->
      FileDownload download =
              transferManager.downloadFile({ b ->
                b.destination(Paths.get("$workFolder/", orderId, keyToDownload)).getObjectRequest({ req -> req.bucket(s3OdpBucketName).key(keyToDownload)
                })
              })
      download.completionFuture().join()
    }
    log.info "Download of $key complete"
  }

  void createZip(OrderInfoDto orderInfoDto, String zipName) {
    String orderId = orderInfoDto.orderId
    runCommand("zip -r -0 ${zipName - '.zip'} ./$orderId/".toString())
  }

  void uploadObjectToS3(OrderInfoDto orderInfoDto, String zipName) {
    runCommand('df -kh')
    String zipPath = "$workFolder/$zipName"
    log.info "Uploading Zip $zipPath to $s3ZipBucketName S3 bucket"
    S3TransferManager transferManager = S3TransferManager.builder().s3ClientConfiguration({ S3ClientConfiguration.Builder cfg ->
      cfg.minimumPartSizeInBytes(50000000)
      if (local) {
        cfg.credentialsProvider(ProfileCredentialsProvider.create(odpProfileName))
      }
    }).build()

    FileUpload upload = transferManager.uploadFile({ UploadFileRequest.Builder b ->
      b.source(Paths.get(zipPath))
              .putObjectRequest({ req -> req.bucket(s3ZipBucketName).key(zipName)
              })
    })
    upload.completionFuture().join()
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

  void cleanUp(String orderId, String zipName) {
    String targetFolder = "$workFolder/$orderId"
    String targetZip = "$workFolder/$zipName"
    log.info "Cleaning up $targetFolder and $targetZip"
    FileUtils.deleteDirectory(new File(targetFolder))
    FileUtils.delete(new File(targetZip))
    runCommand('df -kh')
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
