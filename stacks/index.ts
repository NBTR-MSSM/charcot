import BackEndPaidAccountStack from './BackEndPaidAccountStack'
import * as sst from '@serverless-stack/resources'
import BackEndOdpStack from './BackEndOdpStack'
import FrontendStack from './FrontEndStack'
import { StackArguments } from '../src/types/charcot.types'

const calculateZipBucketName = (stage: string) => {
  const bucketSuffix = stage === 'prod' ? '' : `-${stage}`
  return `${process.env.CEREBRUM_IMAGE_ZIP_BUCKET_NAME}${bucketSuffix}`
}

export default function main(app: sst.App): void {
  // Set default runtime for all functions
  app.setDefaultFunctionProps({
    runtime: 'nodejs16.x'
  })

  const backEndPaidAccountStackArgs: StackArguments = {
    zipBucketName: calculateZipBucketName(app.stage)
  }
  const backEndPaidAccountStack = new BackEndPaidAccountStack(app, 'backend-paid-account', {}, backEndPaidAccountStackArgs)

  let backEndOdpStackArgs: StackArguments = {
    handleCerebrumImageTransfer: backEndPaidAccountStack.handleCerebrumImageTransfer,
    handleCerebrumImageFulfillment: backEndPaidAccountStack.handleCerebrumImageFulfillment
  }
  if (process.env.IS_DEPLOY_SCRIPT) {
    backEndOdpStackArgs = {}
  }
  backEndOdpStackArgs.zipBucketName = backEndPaidAccountStackArgs.zipBucketName

  // eslint-disable-next-line no-new
  new BackEndOdpStack(app, 'backend-odp', {}, backEndOdpStackArgs)

  let frontEndStackArgs: StackArguments = {
    api: backEndPaidAccountStack.api
  }
  if (process.env.IS_DEPLOY_SCRIPT) {
    frontEndStackArgs = {}
  }

  // eslint-disable-next-line no-new
  new FrontendStack(app, 'frontend', {}, frontEndStackArgs)
}
