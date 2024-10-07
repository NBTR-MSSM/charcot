@Grab('org.codehaus.groovy.modules.http-builder:http-builder:0.7.2')import groovy.cli.commons.CliBuilder
import groovyx.net.http.RESTClient
import org.slf4j.Logger
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.ScanResponse
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse

/*
 * Retrofit the filesProcessed of requests/
 */
evaluate(new File('./ScriptUtil.groovy'))
def su = new ScriptUtil()
Logger logger = su.logger(this)

def cli = buildCli()
def opts = cli.parse(this.args)

if (!opts) {
  return
}

if (opts.h) {
  cli.usage()
}
def table = "$opts.stage-charcot-cerebrum-image-order"
DynamoDbClient dynamoDB = DynamoDbClient.builder().build()

def url = 'https://api.mountsinaicharcot.org/cerebrum-images'
def client = new RESTClient(url)
def scanRequest = ScanRequest.builder().tableName(table).build()
while (true) {
  ScanResponse scanResponse = dynamoDB.scan(scanRequest as ScanRequest)
  scanResponse.items.each { Map<String, AttributeValue> fields ->
    // 10/01/2024: Correct orders that got placed while bug was prevalent where
    //   filesProcessed (and order remarks too) of current Zip being process overwrite
    //   previous' ones. Thus request record at the end contains the filesProcessed and order
    //   remarks of last Zip processed.
    if (fields.status?.s() != 'processed' || fields.created.n().toLong() < 1718495874000) {
      return
    }

    def orderId = fields.orderId.s()
    def filter = fields.filter.s()
    // Invoke endpoint to get list of files based on the query specified
    try {
      client.get(query: [filter: fields.filter.s()]) { resp, json ->
        List<String> files = json.collect {
          it.fileName
        }
        logger.debug "JMQ: $orderId filter $filter yielded $files"
        updateFiles(dynamoDB, table, orderId, files, logger)
      }
    } catch (Exception e) {
      logger.error "JMQ: $orderId filter $filter failed:", e
    }
  }
  if (!scanResponse.lastEvaluatedKey()) {
    break
  }
  scanRequest = ScanRequest.builder().tableName(table).exclusiveStartKey(scanResponse.lastEvaluatedKey()).build()
}

void updateFiles(DynamoDbClient dynamoDB, String tableName, String orderId, List<String> files, Logger logger) {
  def filesAttributeValueUpdate = AttributeValue.builder().l(deDupFileNames(dynamoDB, tableName, orderId, files).collect { AttributeValue.builder().s(it).build() }).build()
  UpdateItemResponse updateItemResponse = dynamoDB.updateItem(UpdateItemRequest.builder()
    .tableName(tableName)
    .expressionAttributeNames(['#status': 'status', '#fileNames': 'fileNames', '#filesProcessed': 'filesProcessed'])
    .expressionAttributeValues([':status': AttributeValue.builder().s('processed').build(), ':fileNames': filesAttributeValueUpdate, ':filesProcessed': filesAttributeValueUpdate])
    .key(['orderId': AttributeValue.builder().s(orderId).build(), recordNumber: AttributeValue.builder().n("0").build()])
    .updateExpression('SET #status = :status, #fileNames = :fileNames, #filesProcessed = :filesProcessed').build() as UpdateItemRequest)
  logger.debug "Updated request $orderId:  ${updateItemResponse.toString()}"
}

private List<String> deDupFileNames(DynamoDbClient dynamoDB, String tableName, String orderId, List<String> filesFromQuery) {
  GetItemResponse response = dynamoDB.getItem(GetItemRequest.builder().tableName(tableName).key(['orderId': AttributeValue.builder().s(orderId).build(), recordNumber: AttributeValue.builder().n("0").build()]).build() as GetItemRequest)
  (response.item().filesProcessed.l()*.s() + filesFromQuery).toSet().toList()
}

private CliBuilder buildCli() {
  def cli = new CliBuilder(usage: this.class.getName() + ' [options]')
  cli.with {
    h longOpt: 'help', 'Show usage information'
    s longOpt: 'stage', argName: 'stage', required: true, args: 1, 'The stage (aka environment)'
  }
  return cli
}
