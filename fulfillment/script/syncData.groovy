import groovy.cli.commons.CliBuilder
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient
import software.amazon.awssdk.services.cognitoidentityprovider.model.AdminCreateUserRequest
import software.amazon.awssdk.services.cognitoidentityprovider.model.AdminCreateUserResponse
import software.amazon.awssdk.services.cognitoidentityprovider.model.AdminSetUserPasswordRequest
import software.amazon.awssdk.services.cognitoidentityprovider.model.CognitoIdentityProviderException
import software.amazon.awssdk.services.cognitoidentityprovider.model.ListUserPoolsRequest
import software.amazon.awssdk.services.cognitoidentityprovider.model.ListUserPoolsResponse
import software.amazon.awssdk.services.cognitoidentityprovider.model.ListUsersRequest
import software.amazon.awssdk.services.cognitoidentityprovider.model.ListUsersResponse
import software.amazon.awssdk.services.cognitoidentityprovider.model.UserPoolDescriptionType
import software.amazon.awssdk.services.cognitoidentityprovider.model.UsernameExistsException
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.ScanResponse
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse

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
String sourceStage = opts.'source-stage'
String targetStage = opts.'target-stage'
loadUsers(sourceStage, targetStage)
loadOrders(sourceStage, targetStage)
// end: main program


/*
 * ROUTINES
 */

private void loadUsers(String sourceStage, String targetStage) {
  CognitoIdentityProviderClient.builder().build().withCloseable { CognitoIdentityProviderClient cognitoClient ->
    try {
      ListUserPoolsRequest request = ListUserPoolsRequest.builder().build()
      ListUserPoolsResponse listPoolsResponse = cognitoClient.listUserPools(request)
      def pools = listPoolsResponse.userPools()
      def sourcePool = extractPool(sourceStage, pools)
      def targetPool = extractPool(targetStage, pools)
      println "Source pool: ${sourcePool.name()} ${sourcePool.id()}"
      println "Target pool: ${targetPool.name()} ${targetPool.id()}"
      ListUsersResponse listUsersResponse = cognitoClient.listUsers(ListUsersRequest.builder()
        .userPoolId(sourcePool.id())
        .build() as ListUsersRequest)
      listUsersResponse.users().each {
        println "User: ${it.username()}"
        String email = it.attributes().find { it.name() == 'email' }.value()
        println "Pool ID is ${targetPool.id()}"
        boolean isUserCreatedOrExistsAlready
        try {
          try {
            AdminCreateUserRequest userRequest = AdminCreateUserRequest.builder()
              .userPoolId(targetPool.id())
              .username(email)
              .userAttributes(it.attributes().findAll { !(it.name() in ['sub']) })
              .messageAction("SUPPRESS")
              .build()
            AdminCreateUserResponse createUserResponse = cognitoClient.adminCreateUser(userRequest)
            println "Created user ${createUserResponse.user().username()} in pool ${targetPool.id()} ${targetPool.name()}"
            isUserCreatedOrExistsAlready = true
          } catch (UsernameExistsException ignored) {
            isUserCreatedOrExistsAlready = true
          }

          // Set a temporary password for user. If user exists already, reset it - this will take care of renewing expired
          // temp passwords because for instance user took to long to log back in
          if (isUserCreatedOrExistsAlready) {
            def setPasswordRequest = AdminSetUserPasswordRequest.builder()
              .userPoolId(targetPool.id())
              .username(email)
              .password('Changeme1!')
              .build()
            cognitoClient.adminSetUserPassword(setPasswordRequest)
          }
        } catch (CognitoIdentityProviderException e) {
          println "Problem creating user $email: ${e.awsErrorDetails().errorMessage()}"
        }
      }
    } catch (CognitoIdentityProviderException e) {
      println e.awsErrorDetails().errorMessage()
      System.exit(1)
    }
  }
}

/**
 * Returns the most recently created pool for the given stage. There might be more than one
 * user pool for the same stage because user pools are not cleaned up when an environment is
 * torn down (yet).
 */
private UserPoolDescriptionType extractPool(String stage, List<UserPoolDescriptionType> pools) {
  // response.userPools() returns an unmodifiable collection, make a copy first,
  // else sort() call below throws UnsupportedOperationException
  ([] + pools).findAll {
    it.name().startsWith(stage)
  }.sort { a, b ->
    b.creationDate() <=> a.creationDate()
  }.first()
}

private void loadOrders(String sourceStage, String targetStage) {
  def table = "$sourceStage-charcot-cerebrum-image-order"
  DynamoDbClient dynamoDB = DynamoDbClient.builder().build()
  ScanRequest scanRequest = ScanRequest.builder().tableName(table).build() as ScanRequest
  while (true) {
    ScanResponse scanResponse = dynamoDB.scan(scanRequest)
    writeOrder(dynamoDB, scanResponse, "$targetStage-charcot-cerebrum-image-order")
    if (!scanResponse.lastEvaluatedKey) {
      break
    }
    scanRequest = ScanRequest.builder().tableName(table).exclusiveStartKey(scanResponse.lastEvaluatedKey).build()
  }
}

private CliBuilder buildCli() {
  def cli = new CliBuilder(usage: this.class.getName() + ' [options]')
  cli.with {
    h longOpt: 'help', 'Show usage information'
    s longOpt: 'source-stage', argName: 'source stage', args: 1, 'The source stage to copy FROM', defaultValue: 'prod'
    t longOpt: 'target-stage', argName: 'target stage', required: true, args: 1, 'The target stage to copy TO'
  }
  return cli
}

private void writeOrder(DynamoDbClient dynamoDB, ScanResponse scanResponse, String table) {
  scanResponse.items.each { Map<String, AttributeValue> fields ->
    String orderId = fields.orderId.s()
    Integer recordNumber = fields.recordNumber.n().toInteger()
    //println "JMQ: Mock updating $orderId, $table with $fields"
    // FIXME: To make this operation truly idempotent, wouldn't it be better to destroy the target order first,
    //  and then replace it? Why? To remove any tables in the target that don't exist in the source.
    Map<String, AttributeValue> orderAttributes = fields.findAll { it.key != 'orderId' && it.key != 'recordNumber' }
    UpdateItemResponse updateItemResponse = dynamoDB.updateItem(UpdateItemRequest.builder()
      .tableName(table)
      .key([orderId: AttributeValue.builder().s(orderId).build(), recordNumber: AttributeValue.builder().n(recordNumber.toString()).build()])
      .updateExpression("""\
              SET ${orderAttributes.collect { entry -> "#$entry.key = :$entry.key"
    }.join((', '))
  }""".stripIndent())
  .expressionAttributeNames(orderAttributes.collectEntries { String name, AttributeValue value -> [("#$name".toString()): name] })
  .expressionAttributeValues(orderAttributes.collectEntries { String name, AttributeValue value -> [(":$name".toString()): value] })
  .build() as UpdateItemRequest)
println "Updated request $orderId:  ${updateItemResponse.toString()}"
}
}
