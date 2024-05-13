import groovy.cli.commons.CliBuilder
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient
import software.amazon.awssdk.services.cognitoidentityprovider.model.AdminInitiateAuthRequest
import software.amazon.awssdk.services.cognitoidentityprovider.model.ListUserPoolsRequest
import software.amazon.awssdk.services.cognitoidentityprovider.model.ListUserPoolsResponse
import software.amazon.awssdk.services.cognitoidentityprovider.model.UserPoolDescriptionType


final Map<String, String> STAGE_APP_CLIENT_ID_CONFIG = [debug: '2kclujbsiogn47hr7hlm14hn0n', jmquij0106: '538kf5r55ifhcq1nkjv38cf3ql']

def cli = buildCli()
def opts = cli.parse(this.args)

if (!opts) {
  return
}

if (opts.h) {
  cli.usage()
  return
}

String stage = opts.stage

try (CognitoIdentityProviderClient cognitoClient = CognitoIdentityProviderClient.builder().build()) {
  ListUserPoolsRequest request = ListUserPoolsRequest.builder().build()
  ListUserPoolsResponse listPoolsResponse = cognitoClient.listUserPools(request)
  def pools = listPoolsResponse.userPools()

  def cognitoPool = extractPool(stage, pools)
  AdminInitiateAuthRequest authRequest = AdminInitiateAuthRequest.builder().clientId(STAGE_APP_CLIENT_ID_CONFIG[stage]).userPoolId(cognitoPool.id()).authFlow("ADMIN_NO_SRP_AUTH")
          .authParameters([USERNAME: 'joquijada2010@gmail.com', PASSWORD: '***REMOVED***']).build()
  def result = cognitoClient.adminInitiateAuth(authRequest).authenticationResult()
  println """\
accessToken = ${result.accessToken()}
idToken = ${result.idToken()}
tokenType = ${result.tokenType()}
expiresIn = ${result.expiresIn()}
refreshToken = ${result.refreshToken()}"""
}

private UserPoolDescriptionType extractPool(String stage, List<UserPoolDescriptionType> pools) {
  // response.userPools() returns an unmodifiable collection, make a copy first,
  // else sort() call below throws UnsupportedOperationException
  ([] + pools).findAll {
    it.name().startsWith(stage)
  }.sort { a, b ->
    b.creationDate() <=> a.creationDate()
  }.first()
}

private CliBuilder buildCli() {
  def cli = new CliBuilder(usage: this.class.getName() + ' [options]')
  cli.with {
    h longOpt: 'help', 'Show usage information'
    s longOpt: 'stage', argName: 'stage', args: 1, required: true, 'The stage of the cognito user pool'
  }
  return cli
}
