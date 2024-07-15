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
  ScanResponse scanResponse = dynamoDB.scan(scanRequest as ScanRequest)
  scanResponse.items.each { Map<String, AttributeValue> fields ->
    def orderId = fields.orderId.s
    OrderInfoDto orderInfoDto = fulfillmentService.retrieveOrderInfo(orderId)
    String fileCount = fields.fileNames.l*.s.size().toString()
    updateFileCount(dynamoDB, table, orderId, fileCount)
    println "JMQ: $orderInfoDto.orderId has fileCount $fileCount"
  }
  if (!scanResponse.lastEvaluatedKey()) {
    break
  }
  scanRequest = ScanRequest.builder().tableName(table).exclusiveStartKey(scanResponse.lastEvaluatedKey())
}

void updateFileCount(DynamoDbClient dynamoDB, String tableName, String orderId, String fileCount) {
  UpdateItemResponse updateItemResponse = dynamoDB
    .updateItem(UpdateItemRequest.builder().tableName(tableName)
    .expressionAttributeNames('#fileCount': 'fileCount')
    .expressionAttributeValues(':fileCount': AttributeValue.builder().n(fileCount).build())
    .key(['orderId': AttributeValue.builder().s(orderId).build(), recordNumber: AttributeValue.builder().n("0").build()])
    .updateExpression('SET #fileCount = :fileCount').build() as UpdateItemRequest)
  println "Updated request $orderId:  ${updateItemResponse.toString()}"
}
