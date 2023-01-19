import BackEndPaidAccountStack from './BackEndPaidAccountStack'
import * as sst from '@serverless-stack/resources'
import BackEndOdpStack from './BackEndOdpStack'
import FrontendStack from './FrontEndStack'
import FulfillmentStack from './FulfillmentStack'
import CommonStack from './CommonStack'
import { Vpc } from 'aws-cdk-lib/aws-ec2'

const calculateZipBucketName = (stage: string) => {
  const bucketSuffix = stage === 'prod' ? '' : `-${stage}`
  return `cerebrum-image-zip${bucketSuffix}`
}

export default function main(app: sst.App): void {
  // Set default runtime for all functions
  app.setDefaultFunctionProps({
    runtime: 'nodejs16.x'
  })

  const stage = app.stage
  const zipBucketName = calculateZipBucketName(stage)

  /*
   * The "if()" below is added to prevent error that arises from VPC created in one account (paid acct) and trying
   * to deploy to a different one (ODP) during cdk synth phase:
   * [Error at /prod-charcot-common] Could not find any VPCs matching {"account":"950869325006","region":"us-east-1","filter":{"vpc-id":"vpc-070ce99cb78860905"},"returnAsymmetricSubnets":true,"lookupRoleArn":"arn:aws:iam::950869325006:role/cdk-hnb659fds-lookup-role-950869325006-us-east-1"}
   */
  let vpc
  if (stage === 'debug' || app.account === '045387143127') {
    const commonStack = new CommonStack(app, 'common', {})
    vpc = process.env.VpcId ? Vpc.fromLookup(commonStack, 'VPC', { vpcId: process.env.VpcId }) : commonStack.vpc
  }
  const backEndPaidAccountStack = new BackEndPaidAccountStack(app, 'backend-paid-account', {})

  const fulfillmentStack = new FulfillmentStack(app, 'fulfillment', {}, {
    cerebrumImageOrderTableArn: process.env.CerebrumImageOrderTableArn || backEndPaidAccountStack.cerebrumImageOrderTableArn,
    cerebrumImageOrderQueueArn: process.env.CerebrumImageOrderQueueArn || backEndPaidAccountStack.cerebrumImageOrderQueueArn,
    cerebrumImageMetadataTableArn: process.env.CerebrumImageMetadataTableArn || backEndPaidAccountStack.cerebrumImageMetadataTableArn,
    vpc,
    zipBucketName
  })

  // eslint-disable-next-line no-new
  new FrontendStack(app, 'frontend', {}, {
    apiEndPoint: process.env.ApiEndpoint || backEndPaidAccountStack.api.customDomainUrl || backEndPaidAccountStack.api.url,
    userPoolId: process.env.UserPoolId || backEndPaidAccountStack.userPoolId,
    userPoolClientId: process.env.UserPoolClientId || backEndPaidAccountStack.userPoolClientId,
    cognitoIdentityPoolId: process.env.CognitoIdentityPoolId || backEndPaidAccountStack.cognitoIdentityPoolId
  })

  // eslint-disable-next-line no-new
  new BackEndOdpStack(app, 'backend-odp', {}, {
    fulfillmentServiceTaskRoleArn: process.env.FulfillmentServiceTaskRoleArn || fulfillmentStack!.fulfillmentServiceTaskRoleArn,
    handleCerebrumImageTransferRoleArn: process.env.HandleCerebrumImageTransferRoleArn || backEndPaidAccountStack!.handleCerebrumImageTransferRoleArn,
    zipBucketName
  })
}
