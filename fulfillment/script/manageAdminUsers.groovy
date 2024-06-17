import groovy.cli.commons.CliBuilder
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient
import software.amazon.awssdk.services.cognitoidentityprovider.model.AdminAddUserToGroupRequest
import software.amazon.awssdk.services.cognitoidentityprovider.model.CognitoIdentityProviderException
import software.amazon.awssdk.services.cognitoidentityprovider.model.CreateGroupRequest
import software.amazon.awssdk.services.cognitoidentityprovider.model.ListUserPoolsRequest
import software.amazon.awssdk.services.cognitoidentityprovider.model.ListUserPoolsResponse
import software.amazon.awssdk.services.cognitoidentityprovider.model.UserPoolDescriptionType


/**
 * Use this script to manage admin users and groups.
 */
def cli = buildCli()
def opts = cli.parse(this.args)

if (!opts || opts.h) {
  cli.usage()
}

// begin: main program
CognitoIdentityProviderClient.builder().build().withCloseable {CognitoIdentityProviderClient cognitoClient ->
  try {
    def pool = extractPool(cognitoClient, opts.s)
    if (opts.c) {
      createGroup(cognitoClient, opts.c, pool)
    } else if (opts.a) {
      opts.as[1].split(',').each {
        addUserToGroup(cognitoClient, it, opts.as[0], pool)
      }
    } else if (opts.r) {
      throw new UnsupportedOperationException('Have not implemented remove user from group functionality yet')
    }
  } catch (CognitoIdentityProviderException e) {
    println e.awsErrorDetails().errorMessage()
    System.exit(1)
  }
}
// end: main program


private void addUserToGroup(CognitoIdentityProviderClient cognitoClient, String user, String group, UserPoolDescriptionType pool) {
  AdminAddUserToGroupRequest addUserToGroupRequest = AdminAddUserToGroupRequest.builder()
    .userPoolId(pool.id())
    .groupName(group)
    .username(user)
    .build()
  cognitoClient.adminAddUserToGroup(addUserToGroupRequest)
  println "Added $user to $group in pool ${pool.id()} ${pool.name()}"
}

private void createGroup(CognitoIdentityProviderClient cognitoClient, String name, UserPoolDescriptionType pool) {
  CreateGroupRequest createGroupRequest = CreateGroupRequest.builder()
    .userPoolId(pool.id())
    .groupName(name)
    .build()
  cognitoClient.createGroup(createGroupRequest)
  println "Created group $name in pool ${pool.id()} ${pool.name()}"
}

private CliBuilder buildCli() {
  def cli = new CliBuilder(usage: this.class.getName() + ' [options]')
  cli.with {
    h longOpt: 'help', 'Show usage information'
    c longOpt: 'create-group', args: 1, argName: 'group name', 'Create user group'
    a longOpt: 'add-users-to-group', args: 2, argName: 'group name=comma-separated list of users to add to group', valueSeparator: '=', 'Adds given user list to group'
    r longOpt: 'remove-users-from-group', args: 2, argName: 'group name=comma-separated list of users to remove from group', valueSeparator: '=', 'Adds given user list to group'
    s longOpt: 'stage', argName: 'stage name', args: 1, required: true, 'Stage in the AWS cloud where user pool exists'
  }
  return cli
}

private UserPoolDescriptionType extractPool(CognitoIdentityProviderClient cognitoClient, String stage) {
  ListUserPoolsRequest request = ListUserPoolsRequest.builder().build()
  ListUserPoolsResponse listPoolsResponse = cognitoClient.listUserPools(request)
  // response.userPools() returns an unmodifiable collection, make a copy first,
  // else sort() call below throws UnsupportedOperationException
  ([] + listPoolsResponse.userPools()).findAll {
    it.name().startsWith(stage)
  }.sort { a, b ->
    b.creationDate() <=> a.creationDate()
  }.first()
}
