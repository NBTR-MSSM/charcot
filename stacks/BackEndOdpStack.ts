import * as sst from '@serverless-stack/resources'
import { Bucket } from '@serverless-stack/resources'
import * as iam from 'aws-cdk-lib/aws-iam'
import { Bucket as S3Bucket } from 'aws-cdk-lib/aws-s3'
import { StackArguments } from '../src/types/charcot.types'

/**
 * This stack defines the Charcot backend portion of the AWS ODP account of Mt Sinai. This stack
 * <strong>depends</strong> on the BackEndPaidAccountStack for AWS paid account to have run first, therefore
 * this should be run after the AWS paid account BackEndPaidAccountStack has been deployed. The
 * reason is that this stack expects as inputs the outputs from BackEndPaidAccountStack, for example
 * ARN's of Lambda's that should be granted permission to write to the ODP image bucket
 * during the image transfer process. The script 'deploy.mjs' orchestrates all of this, see that
 * for more details.
 */
export default class BackEndOdpStack extends sst.Stack {
  constructor(scope: sst.App, id: string, props: sst.StackProps, args: StackArguments) {
    super(scope, id, props)

    const fulfillmentRoleArn = args.fulfillmentServiceTaskRoleArn!
    const imgTransferRoleArn = args.handleCerebrumImageTransferRoleArn!

    const stage = this.stage
    // See comment in BackEndPaidAccountStack.ts for the reason of this logic,
    // same applies here
    const bucketSuffix = stage === 'prod' ? '' : `-${stage}`
    const cerebrumImageOdpBucketName = `nbtr-production${bucketSuffix}`

    const cerebrumImageZipBucketName = args.zipBucketName!

    // Buckets
    let cerebrumImageOdpBucket
    if (stage === 'prod') {
      // In PROD the brain image bucket already exists, so just load it
      cerebrumImageOdpBucket = S3Bucket.fromBucketName(this, 'ODPBucketLoadedByName', cerebrumImageOdpBucketName)
    } else {
      cerebrumImageOdpBucket = new Bucket(this, cerebrumImageOdpBucketName, {
        name: cerebrumImageOdpBucketName
      }).cdk.bucket
    }

    cerebrumImageOdpBucket.addToResourcePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        principals: [new iam.ArnPrincipal(imgTransferRoleArn)],
        actions: ['s3:PutObject'],
        resources: [`${cerebrumImageOdpBucket.bucketArn}/*`]
      }))

    const cerebrumImageZipBucket = new Bucket(this, cerebrumImageZipBucketName, {
      name: cerebrumImageZipBucketName
    })

    /*
     * Grant fulfillment service perms to put Zip file in
     * destination bucket. The 's3:GetObject' is needed to allow for the
     * signed URL download
     */
    cerebrumImageZipBucket.cdk.bucket.addToResourcePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        principals: [new iam.ArnPrincipal(fulfillmentRoleArn)],
        actions: ['s3:GetObject', 's3:PutObject'],
        resources: [`${cerebrumImageZipBucket.bucketArn}/*`]
      }))

    /*
     * Also grant fulfillment perms to read the images that are to be zipped. The ListObject policy is needed
     * because the Zip operation needs to list out contents of folders in order to build final
     * list of objects to Zip
     */
    cerebrumImageOdpBucket.addToResourcePolicy(
      new iam.PolicyStatement({
        sid: 'Allow Charcot Fulfillment Service to Read Objects',
        effect: iam.Effect.ALLOW,
        principals: [new iam.ArnPrincipal(fulfillmentRoleArn)],
        actions: ['s3:GetObject'],
        resources: [`${cerebrumImageOdpBucket.bucketArn}/*`]
      }))

    cerebrumImageOdpBucket.addToResourcePolicy(
      new iam.PolicyStatement({
        sid: 'Allow Charcot Fulfillment Service to List Objects',
        effect: iam.Effect.ALLOW,
        principals: [new iam.ArnPrincipal(fulfillmentRoleArn)],
        actions: ['s3:ListBucket'],
        resources: [`${cerebrumImageOdpBucket.bucketArn}`]
      }))

    this.addOutputs({
      CerebrumImageOdpBucketName: cerebrumImageOdpBucketName
    })
  }
}
