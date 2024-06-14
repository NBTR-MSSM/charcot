import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.amazonaws.services.sqs.model.PurgeQueueRequest
import com.amazonaws.services.sqs.model.SendMessageRequest
import groovy.cli.commons.CliBuilder
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest

/**
 * Use this script to copy user and order data from one environment to the other.
 */
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
println "stage is $stage"
println "requests is $requests"

def queueUrl = "https://sqs.us-east-1.amazonaws.com/045387143127/$stage-charcot-cerebrum-image-order-queue"
purgeSqs(queueUrl)
requests.each {
  resetStatus(it, "$stage-charcot-cerebrum-image-order", dynamoDB)
  resubmitToSqs(it, queueUrl)
  println "Request $it will get reprocessed"
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

void resetStatus(String requestId, String tableName, DynamoDbClient dynamoDB) {
  dynamoDB.updateItem(UpdateItemRequest.builder().tableName(tableName)
          .key([orderId: AttributeValue.builder().s(requestId).build(), recordNumber: AttributeValue.builder().n('0').build()])
          .expressionAttributeNames(['#status': 'status'])
          .expressionAttributeValues([':status': AttributeValue.builder().s('received').build()])
          .updateExpression('SET #status = :status')
          .build() as UpdateItemRequest)
  println "Updated status for $requestId"
}

void resubmitToSqs(String requestId, String queueUrl) {
  AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient()
  sqs.sendMessage(new SendMessageRequest().withQueueUrl(queueUrl).withMessageBody(/{"orderId":"$requestId"}/))
}

void purgeSqs(String queueUrl) {
  AmazonSQSClientBuilder.defaultClient().purgeQueue(new PurgeQueueRequest(queueUrl))
}
