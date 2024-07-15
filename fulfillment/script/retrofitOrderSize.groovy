import org.mountsinaicharcot.fulfillment.dto.OrderInfoDto
import org.mountsinaicharcot.fulfillment.service.FulfillmentService
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.ScanResponse
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse

/*
 * Retrofit the order size in bytes, by looking at list of files processed
 * and then querying S3 to get a sum of the size of each file per order, then
 * update the order size in DynamoDB order table
 */
def table = 'prod-charcot-cerebrum-image-order'
DynamoDbClient dynamoDB = DynamoDbClient.builder().build()
def fulfillmentService = new FulfillmentService(
  dynamoDbOrderTableName: table,
  s3OdpBucketName: 'nbtr-production'
  )

def scanRequest = ScanRequest.builder().tableName(table).build()
while (true) {
  ScanResponse scanResponse = dynamoDB.scan(scanRequest)
  scanResponse.items.each { Map<String, AttributeValue> fields ->
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
  if (!scanResponse.lastEvaluatedKey) {
    break
  }
  scanRequest = ScanRequest.builder().tableName(table).exclusiveStartKey(scanResponse.lastEvaluatedKey).build()
}
