import groovy.cli.commons.CliBuilder
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.ScanResponse
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse

/**
 * This script is for migrating order data to the new table that contains the sortKey of 'recordNumber'. By design, DynamoDB
 * does not allow adding sortKey after a table has been created, so had to
 *   1. deploy stack to create the new table
 *   2. copy the data to new table
 *   3. create a backup of new table
 *   4. drop old table
 *   5. restore back up of old table with the old table name
 *
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
migrateOrders(opts.stage, opts.'from-table', opts.'to-table')
// end: main program


/*
 * ROUTINES
 */
private void migrateOrders(String stage, String fromTable, String toTable) {
  def table = "$stage-$fromTable"
  DynamoDbClient dynamoDB = DynamoDbClient.builder().build()
  ScanRequest scanRequest = ScanRequest.builder().tableName(table).build() as ScanRequest
  while (true) {
    ScanResponse scanResponse = dynamoDB.scan(scanRequest)
    writeOrder(dynamoDB, scanResponse, "$stage-$toTable")
    if (!scanResponse.lastEvaluatedKey()) {
      break
    }
    scanRequest = ScanRequest.builder().tableName(table).exclusiveStartKey(scanResponse.lastEvaluatedKey()).build() as ScanRequest
  }
}

private CliBuilder buildCli() {
  def cli = new CliBuilder(usage: this.class.getName() + ' [options]')
  cli.with {
    h longOpt: 'help', 'Show usage information'
    s longOpt: 'stage', argName: 'stage', required: true, args: 1, 'The stage (aka environment) on which to migrate the data from table A to table B', defaultValue: 'prod'
    f longOpt: 'from-table', argName: 'table to copy data from', required: true, args: 1, 'Table to copy data from'
    t longOpt: 'to-table', argName: 'table to copy data to', required: true, args: 1, 'Table to copy data to'
  }
  return cli
}

private void writeOrder(DynamoDbClient dynamoDB, ScanResponse scanResponse, String table) {
  scanResponse.items().each { Map<String, AttributeValue> fields ->
    String orderId = fields.orderId.s
    Map<String, AttributeValue> orderAttributes = fields.findAll { it.key != 'orderId' && it.key != 'recordNumber' }
    UpdateItemResponse updateItemResponse = dynamoDB.updateItem(UpdateItemRequest.builder().tableName(table).key([orderId: AttributeValue.builder().s(orderId).build(), recordNumber: AttributeValue.builder().n('0').build()])
    .updateExpression("""\
              SET ${orderAttributes.collect { String name, AttributeValue value -> "#$name = :$name"
  }.join((', '))
}""".stripIndent())
.expressionAttributeNames(orderAttributes.collectEntries { String name, AttributeValue value -> [("#$name".toString()): name] })
.expressionAttributeValues(orderAttributes.collectEntries { String name, AttributeValue value -> [(":$name".toString()): value] })
.build() as UpdateItemRequest)
println "Updated request $orderId: ${updateItemResponse.toString()}"
}
}
