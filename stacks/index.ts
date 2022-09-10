import BackEndPaidAccountStack from './BackEndPaidAccountStack'
import * as sst from '@serverless-stack/resources'
import BackEndOdpStack from './BackEndOdpStack'
import FrontendStack from './FrontEndStack'
import FulfillmentStack from './FulfillmentStack'
import CommonStack from './CommonStack'
import { SubnetFilter, Vpc } from 'aws-cdk-lib/aws-ec2'

const calculateZipBucketName = (stage: string) => {
  const bucketSuffix = stage === 'prod' ? '' : `-${stage}`
  return `${process.env.CEREBRUM_IMAGE_ZIP_BUCKET_NAME}${bucketSuffix}`
}

export default function main(app: sst.App): void {
  // Set default runtime for all functions
  app.setDefaultFunctionProps({
    runtime: 'nodejs14.x'
  })

  const stage = app.stage
  const zipBucketName = calculateZipBucketName(stage)
  let backEndPaidAccountStack: BackEndPaidAccountStack
  let fulfillmentStack: FulfillmentStack
  if (stage === 'debug' || app.account === '045387143127') {
    const commonStack = new CommonStack(app, 'common', {})

    const vpc = process.env.VpcId ? Vpc.fromLookup(commonStack, 'VPC', { vpcId: process.env.VpcId }) : commonStack.vpc

    backEndPaidAccountStack = new BackEndPaidAccountStack(app, 'backend-paid-account', {}, {
      zipBucketName,
      vpc
    })

    fulfillmentStack = new FulfillmentStack(app, 'fulfillment', {}, {
      cerebrumImageOrderTableArn: backEndPaidAccountStack.cerebrumImageOrderTableArn,
      vpc,
      zipBucketName
    })

    // eslint-disable-next-line no-new
    new FrontendStack(app, 'frontend', {}, {
      apiEndPoint: process.env.ApiEndpoint || backEndPaidAccountStack.api.url,
      userPoolId: process.env.UserPoolId || backEndPaidAccountStack.userPoolId,
      userPoolClientId: process.env.UserPoolClientId || backEndPaidAccountStack.userPoolClientId,
      cognitoIdentityPoolId: process.env.CognitoIdentityPoolId || backEndPaidAccountStack.cognitoIdentityPoolId
    })
  }
  if (stage === 'debug' || app.account === '950869325006') {
    // eslint-disable-next-line no-new
    new BackEndOdpStack(app, 'backend-odp', {}, {
      fulfillmentServiceTaskRoleArn: process.env.FulfillmentServiceTaskRoleArn || fulfillmentStack!.fulfillmentServiceTaskRoleArn,
      handleCerebrumImageTransferRoleArn: process.env.HandleCerebrumImageTransferRoleArn || backEndPaidAccountStack!.handleCerebrumImageTransferRoleArn,
      zipBucketName
    })
  }
}
