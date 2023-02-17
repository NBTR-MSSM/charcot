import * as sst from 'sst/constructs'
import { Certificate } from 'aws-cdk-lib/aws-certificatemanager'
import * as route53 from 'aws-cdk-lib/aws-route53'
import { BackEndPaidAccountStack } from './BackEndPaidAccountStack'
import { use } from 'sst/constructs'

export function FrontendStack({ stack }: sst.StackContext) {
  const {
    api,
    userPoolId,
    userPoolClientId,
    cognitoIdentityPoolId
  } = use(BackEndPaidAccountStack)

  // Define our React app
  const environment = {
    REACT_APP_API_URL: process.env.ApiEndpoint || api.customDomainUrl || api.url,
    REACT_APP_REGION: stack.region,
    REACT_APP_USER_POOL_ID: userPoolId!,
    REACT_APP_USER_POOL_CLIENT_ID: userPoolClientId!,
    REACT_APP_IDENTITY_POOL_ID: cognitoIdentityPoolId!
  }
  const stage = stack.stage
  const hostedZone = route53.HostedZone.fromHostedZoneAttributes(stack, 'HostedZone', {
    hostedZoneId: 'Z0341163303ASZWMW1YTS',
    zoneName: 'mountsinaicharcot.org'
  })
  const site = new sst.StaticSite(stack, 'ReactSite', {
    path: 'frontend',
    buildCommand: 'npm run build',
    buildOutput: 'build',
    environment,
    customDomain: {
      domainName: stage === 'prod' ? 'www.mountsinaicharcot.org' : `${stage}.mountsinaicharcot.org`,
      domainAlias: stage === 'prod' ? 'mountsinaicharcot.org' : undefined,
      cdk: {
        hostedZone,
        certificate: Certificate.fromCertificateArn(stack, 'MyCert', 'arn:aws:acm:us-east-1:045387143127:certificate/1004f57f-a544-476d-8a31-5b878a71c276')
      }
    }
  })

  // Show the url in the output and the environment used for this React app
  stack.addOutputs({
    SiteUrl: (site.customDomainUrl || site.url) as string,
    DistributionDomain: site.cdk.distribution.domainName,
    DistributionId: site.cdk.distribution.distributionId,
    Environment: JSON.stringify(environment, null, 2)
  })
}
