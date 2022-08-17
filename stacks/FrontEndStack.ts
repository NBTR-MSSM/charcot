import * as sst from '@serverless-stack/resources'
import { StackArguments } from '../src/types/charcot.types'
import { Certificate } from 'aws-cdk-lib/aws-certificatemanager'
import * as route53 from 'aws-cdk-lib/aws-route53'

export default class FrontendStack extends sst.Stack {
  constructor(scope: sst.App, id: string, props: sst.StackProps, args: StackArguments) {
    super(scope, id, props)
    const { apiEndPoint, userPoolId, userPoolClientId, cognitoIdentityPoolId } = args

    // Define our React app
    const environment = {
      REACT_APP_API_URL: apiEndPoint!,
      REACT_APP_REGION: scope.region,
      REACT_APP_USER_POOL_ID: userPoolId!,
      REACT_APP_USER_POOL_CLIENT_ID: userPoolClientId!,
      REACT_APP_IDENTITY_POOL_ID: cognitoIdentityPoolId!
    }
    const stage = this.stage
    const hostedZone = route53.HostedZone.fromHostedZoneAttributes(this, 'HostedZone', {
      hostedZoneId: 'Z0341163303ASZWMW1YTS',
      zoneName: 'mountsinaicharcot.org'
    })
    const site = new sst.ReactStaticSite(this, 'ReactSite', {
      path: 'frontend',
      environment,
      customDomain: {
        domainName: stage === 'prod' ? 'www.mountsinaicharcot.org' : `${stage}.mountsinaicharcot.org`,
        domainAlias: stage === 'prod' ? 'mountsinaicharcot.org' : undefined,
        cdk: {
          hostedZone,
          certificate: Certificate.fromCertificateArn(this, 'MyCert', 'arn:aws:acm:us-east-1:045387143127:certificate/1004f57f-a544-476d-8a31-5b878a71c276')
        }
      }
    })

    // Show the url in the output and the environment used for this React app
    this.addOutputs({
      SiteUrl: site.customDomainUrl || site.url,
      DistributionDomain: site.distributionDomain,
      DistributionId: site.distributionId,
      Environment: JSON.stringify(environment, null, 2)
    })
  }
}
