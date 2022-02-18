import * as sst from '@serverless-stack/resources'
import { StackArguments } from '../src/types/charcot.types'

export default class FrontendStack extends sst.Stack {
  constructor(scope: sst.App, id: string, props: sst.StackProps, args: StackArguments) {
    super(scope, id, props)
    const { api } = args
    // Define our React app
    const site = new sst.ReactStaticSite(this, 'ReactSite', {
      path: 'frontend',
      environment: {
        // The 'foo' needed because SST does pre-processing of all stacks even
        // when deploying an unrelated stack. This affects deploy.msj deploy context.
        REACT_APP_API_URL: api?.url || process.env.ApiEndpoint as string || 'foo',
        REACT_APP_REGION: scope.region /* ,
        REACT_APP_BUCKET: bucket.bucketName,
        REACT_APP_USER_POOL_ID: auth.cognitoUserPool.userPoolId,
        REACT_APP_IDENTITY_POOL_ID: auth.cognitoCfnIdentityPool.ref,
        REACT_APP_USER_POOL_CLIENT_ID:
        auth.cognitoUserPoolClient.userPoolClientId */
      }
    })

    // Show the url in the output
    this.addOutputs({
      SiteUrl: site.url
    })
  }
}
