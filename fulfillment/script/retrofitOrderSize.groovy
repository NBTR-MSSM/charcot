import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.dynamodbv2.model.ScanResult
import org.mountsinaicharcot.fulfillment.dto.OrderInfoDto
import org.mountsinaicharcot.fulfillment.service.FulfillmentService

/*
 * Retrofit the order size in bytes, by looking at list of files processed
 * and then querying S3 to get a sum of the size of each file per order, then
 * update the order size in DynamoDB order table
 */
def table = 'prod-charcot-cerebrum-image-order'
AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient()
def scanRequest = new ScanRequest()
def fulfillmentService = new FulfillmentService(
        dynamoDbOrderTableName: table,
        s3OdpBucketName: 'nbtr-production'
)

scanRequest.tableName = table
while (true) {
  ScanResult result = dynamoDB.scan(scanRequest)
  result.items.each { Map<String, AttributeValue> fields ->
    if (fields.size) {
      return
    }
    def orderId = fields.orderId.s
    OrderInfoDto orderInfoDto = fulfillmentService.retrieveOrderInfo(orderId)
    orderInfoDto.fileNames = fields.filesProcessed.l*.s
    fulfillmentService.calculateOrderSizeAndPartitionIntoBuckets(orderInfoDto)
    println "JMQ: $orderInfoDto.orderId has size $orderInfoDto.size"
    fulfillmentService.recordOrderSize(orderId, orderInfoDto.size)
  }
  if (!result.lastEvaluatedKey) {
    break
  }
  scanRequest.exclusiveStartKey = result.lastEvaluatedKey
}

