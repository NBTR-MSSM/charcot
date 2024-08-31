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

CognitoIdentityProviderClient.builder().build().withCloseable { CognitoIdentityProviderClient cognitoClient ->
  ListUserPoolsRequest request = ListUserPoolsRequest.builder().build()
  ListUserPoolsResponse listPoolsResponse = cognitoClient.listUserPools(request)
  def pools = listPoolsResponse.userPools()

  def cognitoPool = extractPool(stage, pools)
  AdminInitiateAuthRequest authRequest = AdminInitiateAuthRequest.builder().clientId(STAGE_APP_CLIENT_ID_CONFIG[stage]).userPoolId(cognitoPool.id()).authFlow("ADMIN_NO_SRP_AUTH")
    .authParameters([USERNAME: opts.username, PASSWORD: opts.password]).build()
  def result = cognitoClient.adminInitiateAuth(authRequest).authenticationResult()
  println """\
    accessToken = ${result.accessToken()}
    idToken = ${result.idToken()}
    tokenType = ${result.tokenType()}
    expiresIn = ${result.expiresIn()}
    refreshToken = ${result.refreshToken()}""".stripIndent()
}

private UserPoolDescriptionType extractPool(String stage, List<UserPoolDescriptionType> pools) {
  // Grab the most recently created pool for this stage
  pools.findAll {
    it.name().startsWith(stage)
  }.sort { pool ->
    -pool.creationDate().toEpochMilli()
  }.first()
}

private CliBuilder buildCli() {
  def cli = new CliBuilder(usage: this.class.getName() + ' [options]')
  cli.with {
    h longOpt: 'help', 'Show usage information'
    s longOpt: 'stage', argName: 'stage', args: 1, required: true, 'The stage of the cognito user pool'
    u longOpt: 'username', argName: 'username', args: 1, required: true, 'Your Charcot username'
    p longOpt: 'password', argName: 'password', args: 1, required: true, 'Your Charcot password'
  }
  return cli
}
