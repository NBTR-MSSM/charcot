import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.amazonaws.services.sqs.model.PurgeQueueRequest
import com.amazonaws.services.sqs.model.SendMessageRequest
import groovy.cli.commons.CliBuilder
import org.slf4j.Logger
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest

/**
 * Use this script reset request status to 'received' for reprocessing. This is useful
 * for orders that failed for one reason or another, and you want to retry it.
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

// begin: main program
DynamoDbClient dynamoDB = DynamoDbClient.builder().build()
String stage = opts.stage
List<String> requests = opts.rs
logger.debug "stage is $stage"
logger.debug "requests is $requests"

def queueUrl = "https://sqs.us-east-1.amazonaws.com/045387143127/$stage-charcot-cerebrum-image-order-queue"
purgeSqs(queueUrl)
requests.each {
  resetStatus(it, "$stage-charcot-cerebrum-image-order", dynamoDB, logger)
  resubmitToSqs(it, queueUrl)
  logger.debug "Request $it will get reprocessed"
}
// end: main program

/*
 * ROUTINES
 */

private CliBuilder buildCli() {
  def cli = new CliBuilder(usage: this.class.getName() + ' [options]')
  cli.with {
    r longOpt: 'request', argName: 'request', required: true, args: 1, 'Request ID'
    s longOpt: 'stage', argName: 'stage', required: true, args: 1, 'The stage name'
  }
  return cli
}

void resetStatus(String requestId, String tableName, DynamoDbClient dynamoDB, Logger logger) {
  String targetStatus = 'received'
  dynamoDB.updateItem(UpdateItemRequest.builder().tableName(tableName)
    .key([orderId: AttributeValue.builder().s(requestId).build(), recordNumber: AttributeValue.builder().n('0').build()])
    .expressionAttributeNames(['#status': 'status'])
    .expressionAttributeValues([':status': AttributeValue.builder().s(targetStatus).build()])
    .updateExpression('SET #status = :status')
    .build() as UpdateItemRequest)
  logger.debug "Updated status for $requestId"
}

void resubmitToSqs(String requestId, String queueUrl) {
  AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient()
  sqs.sendMessage(new SendMessageRequest().withQueueUrl(queueUrl).withMessageBody(/{"orderId":"$requestId"}/))
}

void purgeSqs(String queueUrl) {
  AmazonSQSClientBuilder.defaultClient().purgeQueue(new PurgeQueueRequest(queueUrl))
}
