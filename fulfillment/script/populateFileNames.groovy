@Grab('org.codehaus.groovy.modules.http-builder:http-builder:0.7.2')
import groovyx.net.http.RESTClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.ScanResponse
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse

/*
 * Retrofit the order size in bytes, by looking at list of files processed
 * and then querying S3 to get a sum of the size of each file per order, and finally
 * update the order size in DynamoDB order table
 */

def table = 'prod-charcot-cerebrum-image-order'
DynamoDbClient dynamoDB = DynamoDbClient.builder().build()

def url = 'https://api.mountsinaicharcot.org/cerebrum-images'
def client = new RESTClient(url)
def scanRequest = ScanRequest.builder().tableName(table).build()
while (true) {
  ScanResponse scanResponse = dynamoDB.scan(scanRequest as ScanRequest)
  scanResponse.items.each { Map<String, AttributeValue> fields ->
    if (fields.status?.s) {
      return
    }

    def orderId = fields.orderId.s
    def filter = fields.filter.s
    // Invoke endpoint to get list of files based on the query specified
    try {
      client.get(query: [filter: fields.filter.s]) { resp, json ->
        List<String> files = json.collect {
          it.fileName
        }
        println "JMQ: $orderId filter $filter yielded $files"
        updateFiles(dynamoDb, table, orderId, files)
      }
    } catch (Exception e) {
      println "JMQ: $orderId filter $filter failed: $e"
    }
  }
  if (!scanResponse.lastEvaluatedKey) {
    break
  }
  scanRequest = ScanRequest.builder().tableName(table).exclusiveStartKey(scanResponse.lastEvaluatedKey())
}

void updateFiles(DynamoDbClient dynamoDB, String tableName, String orderId, List<String> files) {
  def filesAttributeValueUpdate = AttributeValue.builder().l(files.collect { AttributeValue.builder().s(it).build() }).build()
  UpdateItemResponse updateItemResponse = dynamoDB.updateItem(UpdateItemRequest.builder()
    .tableName(tableName)
    .expressionAttributeNames(['#status': 'status', '#fileNames': 'fileNames', '#filesProcessed': 'filesProcessed'])
    .expressionAttributeValues(['status': AttributeValue.builder().s('processed').build(), 'fileNames': filesAttributeValueUpdate, 'filesProcessed': filesAttributeValueUpdate])
    .key(['orderId': AttributeValue.builder().s(orderId).build(), recordNumber: AttributeValue.builder().n("0").build()])
    .updateExpression('SET #status = :status, #fileNames = :fileNames, #filesProcessed = :filesProcessed').build() as UpdateItemRequest)
  println "Updated request $orderId:  ${updateItemResponse.toString()}"
}
