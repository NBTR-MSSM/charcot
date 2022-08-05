import * as sst from '@serverless-stack/resources'
import { StackArguments } from '../src/types/charcot.types'

export default class FrontendStack extends sst.Stack {
  constructor(scope: sst.App, id: string, props: sst.StackProps, args: StackArguments) {
    super(scope, id, props)
    const { api, auth } = args
    // Define our React app
    const environment = {
      // The 'foo' needed because SST does pre-processing of all stacks even
      // when deploying an unrelated stack. This is mainly to keep
      // deploy.msj happy.
      REACT_APP_API_URL: api?.url || process.env.ApiEndpoint as string || 'foo',
      REACT_APP_REGION: scope.region,
      // REACT_APP_BUCKET: bucket.bucketName,
      REACT_APP_USER_POOL_ID: auth?.userPoolId || process.env.UserPoolId! || 'foo',
      REACT_APP_USER_POOL_CLIENT_ID: auth?.userPoolClientId || process.env.UserPoolClientId! || 'foo',
      REACT_APP_IDENTITY_POOL_ID: auth?.cognitoIdentityPoolId || process.env.IdentityPoolId! || 'foo'
    }
    const site = new sst.ReactStaticSite(this, 'ReactSite', {
      path: 'frontend',
      environment
    })

    // Show the url in the output and the environment used for thi React app
    this.addOutputs({
      SiteUrl: site.url,
      DistributionDomain: site.distributionDomain,
      DistributionId: site.distributionId,
      Environment: JSON.stringify(environment, null, 2)
    })
  }
}
