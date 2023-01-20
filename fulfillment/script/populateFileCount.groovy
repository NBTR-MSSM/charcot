import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.dynamodbv2.model.ScanResult
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult
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
    def orderId = fields.orderId.s
    OrderInfoDto orderInfoDto = fulfillmentService.retrieveOrderInfo(orderId)
    String fileCount = fields.fileNames.l*.s.size().toString()
    updateFileCount(table, dynamoDB, orderId, fileCount)
    println "JMQ: $orderInfoDto.orderId has fileCount $fileCount"
  }
  if (!result.lastEvaluatedKey) {
    break
  }
  scanRequest.exclusiveStartKey = result.lastEvaluatedKey
}

void updateFileCount(String tableName, AmazonDynamoDB dynamoDB, String orderId, String fileCount) {
  def attributeValueUpdate = new AttributeValueUpdate().withValue(new AttributeValue().withN(fileCount))
  UpdateItemResult updateItemResult = dynamoDB.updateItem(tableName,
          [orderId: new AttributeValue().withS(orderId)],
          [fileCount: attributeValueUpdate])
  println "Updated request $orderId:  ${updateItemResult.toString()}"
}


