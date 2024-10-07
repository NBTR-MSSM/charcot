import groovy.cli.commons.CliBuilder
import org.mountsinaicharcot.fulfillment.dto.OrderInfoDto
import org.mountsinaicharcot.fulfillment.service.FulfillmentService
import org.mountsinaicharcot.fulfillment.service.OrderService
import org.slf4j.Logger
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.ScanResponse

/*
 * Retrofit the order size in bytes, by looking at list of files processed
 * and then querying S3 to get a sum of the size of each file per order, then
 * update the order size in DynamoDB order table
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

def table = "${opts.stage}-charcot-cerebrum-image-order"
DynamoDbClient dynamoDB = DynamoDbClient.builder().build()
def fulfillmentService = new FulfillmentService(
  s3OdpBucketName: 'nbtr-production'
  )

def orderService = new OrderService(
  dynamoDbOrderTableName: table,
  )

def scanRequest = ScanRequest.builder().tableName(table).build()
while (true) {
  ScanResponse scanResponse = dynamoDB.scan(scanRequest)
  scanResponse.items().each { Map<String, AttributeValue> fields ->
    if (fields.status?.s() != 'processed' || fields.created.n().toLong() < 1718495874000) {
      return
    }
    /*if (fields.size()) {
     return
     }*/
    def orderId = fields.orderId.s
    OrderInfoDto orderInfoDto = orderService.retrieveOrderInfo(orderId)
    orderInfoDto.fileNames = fields.filesProcessed.l()*.s()
    fulfillmentService.calculateOrderSizeAndPartitionIntoBuckets(orderInfoDto)
    logger.debug "JMQ: $orderInfoDto.orderId has size $orderInfoDto.size"
    orderService.recordOrderSize(orderId, orderInfoDto.size)
  }
  if (!scanResponse.lastEvaluatedKey()) {
    break
  }
  scanRequest = ScanRequest.builder().tableName(table).exclusiveStartKey(scanResponse.lastEvaluatedKey).build()
}

private CliBuilder buildCli() {
  def cli = new CliBuilder(usage: this.class.getName() + ' [options]')
  cli.with {
    h longOpt: 'help', 'Show usage information'
    s longOpt: 'stage', argName: 'stage', required: true, args: 1, 'The stage (aka environment)'
  }
  return cli
}
