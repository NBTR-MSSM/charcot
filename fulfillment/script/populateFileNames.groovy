@Grab('org.codehaus.groovy.modules.http-builder:http-builder:0.7.2')
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.dynamodbv2.model.ScanResult
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult
import groovyx.net.http.RESTClient

/*
 * Retrofit the order size in bytes, by looking at list of files processed
 * and then querying S3 to get a sum of the size of each file per order, then
 * update the order size in DynamoDB order table
 */

def table = 'prod-charcot-cerebrum-image-order'
AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient()
def scanRequest = new ScanRequest()
def url = 'https://api.mountsinaicharcot.org/cerebrum-images'
def client = new RESTClient(url)
scanRequest.tableName = table
while (true) {
  ScanResult result = dynamoDB.scan(scanRequest)
  result.items.each { Map<String, AttributeValue> fields ->
    if (fields.status?.s) {
      return
    }

    def orderId = fields.orderId.s
    def filter = fields.filter.s
    // Invoke endpoint to get list of files
    try {
      client.get(query: [filter: fields.filter.s]) { resp, json ->
        List<String> files = json.collect {
          it.fileName
        }
        println "JMQ: $orderId filter $filter yielded $files"
        updateFiles(table, dynamoDB, orderId, files)
      }
    } catch (Exception e) {
      println "JMQ: $orderId filter $filter failed: $e"
    }
  }
  if (!result.lastEvaluatedKey) {
    break
  }
  scanRequest.exclusiveStartKey = result.lastEvaluatedKey
}

void updateFiles(String tableName, AmazonDynamoDB dynamoDB, String orderId, List<String> files) {
  def attributeValueUpdate = new AttributeValueUpdate().withValue(new AttributeValue().withL(files.collect { new AttributeValue().withS(it) }))
  UpdateItemResult updateItemResult = dynamoDB.updateItem(tableName,
          [orderId: new AttributeValue().withS(orderId)],
          [status        : new AttributeValueUpdate().withValue(new AttributeValue().withS('processed')),
           fileNames     : attributeValueUpdate,
           filesProcessed: attributeValueUpdate])
  println "Updated request $orderId:  ${updateItemResult.toString()}"
}

