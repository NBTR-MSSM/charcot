import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.dynamodbv2.model.ScanResult
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult
import groovy.cli.commons.CliBuilder
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient
import software.amazon.awssdk.services.cognitoidentityprovider.model.AdminCreateUserRequest
import software.amazon.awssdk.services.cognitoidentityprovider.model.ListUserPoolsRequest
import software.amazon.awssdk.services.cognitoidentityprovider.model.ListUserPoolsResponse
import software.amazon.awssdk.services.cognitoidentityprovider.model.CognitoIdentityProviderException
import software.amazon.awssdk.services.cognitoidentityprovider.model.ListUsersRequest
import software.amazon.awssdk.services.cognitoidentityprovider.model.ListUsersResponse
import software.amazon.awssdk.services.cognitoidentityprovider.model.UserPoolDescriptionType
import software.amazon.awssdk.services.cognitoidentityprovider.model.AdminCreateUserResponse

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
// loadOrders(sourceStage, targetStage)
// end: main program


/*
 * ROUTINES
 */

private void loadUsers(String sourceStage, String targetStage) {
  try (CognitoIdentityProviderClient cognitoClient = CognitoIdentityProviderClient.builder().build()) {
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
      String email = it.attributes().find { it.name() == 'email'}.value()
      AdminCreateUserRequest userRequest = AdminCreateUserRequest.builder()
              .userPoolId(targetPool.id())
              .username(email)
              .temporaryPassword('Changeme1+')
              .userAttributes(it.attributes().findAll { it.name() !in ['sub'] })
              .messageAction("SUPPRESS")
              .build()
      try {
        AdminCreateUserResponse createUserResponse = cognitoClient.adminCreateUser(userRequest)
        println "Created user ${createUserResponse.user().username()} in pool ${targetPool.id()} ${targetPool.name()}"
      } catch (CognitoIdentityProviderException e) {
        println "Problem creating user $email: ${e.awsErrorDetails().errorMessage()}"
      }
    }
  } catch (CognitoIdentityProviderException e) {
    println e.awsErrorDetails().errorMessage()
    System.exit(1)
  }
}

/**
 * Returns the most recently created pool for the given stage. There might be more than one
 * user pool for the same stage because user pools are not cleaned up when an environment is
 * brought torn down (yet).
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
  AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient()
  def scanRequest = new ScanRequest()
  scanRequest.tableName = table
  while (true) {
    ScanResult result = dynamoDB.scan(scanRequest)
    writeOrder(dynamoDB, result, "$targetStage-charcot-cerebrum-image-order")
    if (!result.lastEvaluatedKey) {
      break
    }
    scanRequest.exclusiveStartKey = result.lastEvaluatedKey
  }
}

private CliBuilder buildCli() {
  def cli = new CliBuilder(usage: this.class.getName() + ' [options]')
  cli.with {
    h longOpt: 'help', 'Show usage information'
    s longOpt: 'source-stage', argName: 'source stage', 'The source stage to copy FROM', defaultValue: 'prod'
    t longOpt: 'target-stage', argName: 'target stage', required: true, args: 1, 'The target stage to copy TO'
  }
  return cli
}

private void writeOrder(AmazonDynamoDB dynamoDB, ScanResult result, String table) {
  result.items.each { Map<String, AttributeValue> fields ->
    String orderId = fields.orderId.s
    //println "JMQ: Mock updating $orderId, $table with $fields"
    UpdateItemResult updateItemResult = dynamoDB.updateItem(table,
            [orderId: new AttributeValue().withS(fields.orderId.s)],
            fields.findAll { it.key != 'orderId' }.collectEntries { String name, AttributeValue value ->
              [(name): new AttributeValueUpdate().withValue(value)]
            })
    println "Updated request $orderId:  ${updateItemResult.toString()}"
  }
}
