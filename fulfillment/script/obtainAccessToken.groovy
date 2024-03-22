import groovy.cli.commons.CliBuilder
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient
import software.amazon.awssdk.services.cognitoidentityprovider.model.AdminInitiateAuthRequest
import software.amazon.awssdk.services.cognitoidentityprovider.model.ListUserPoolsRequest
import software.amazon.awssdk.services.cognitoidentityprovider.model.ListUserPoolsResponse
import software.amazon.awssdk.services.cognitoidentityprovider.model.UserPoolDescriptionType


final Map<String, String> STAGE_APP_CLIENT_ID_CONFIG = [debug: '34f6be8e87o0vobugfcajvo87j']

def cli = buildCli()
def opts = cli.parse(this.args)

if (!opts) {
  return
}

if (opts.h) {
  cli.usage()
}

String stage = opts.stage

try (CognitoIdentityProviderClient cognitoClient = CognitoIdentityProviderClient.builder().build()) {
  ListUserPoolsRequest request = ListUserPoolsRequest.builder().build()
  ListUserPoolsResponse listPoolsResponse = cognitoClient.listUserPools(request)
  def pools = listPoolsResponse.userPools()

  def cognitoPool = extractPool('debug', pools)
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
    s longOpt: 'stage', argName: 'stage', args: 1, 'The stage of the cognito user pool', defaultValue: 'debug'
  }
  return cli
}
