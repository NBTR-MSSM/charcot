import * as sst from '@serverless-stack/resources'
import { Bucket } from '@serverless-stack/resources'
import * as iam from '@aws-cdk/aws-iam'

export default class CharcotStackOdp extends sst.Stack {
  constructor(scope: sst.App, id: string, props?: sst.StackProps) {
    super(scope, id, props)

    const stage = this.stage

    // See comment in CharcotStack.ts for the reason of this logic,
    // same applies here
    const bucketStage = stage === 'prod' ? '' : `-${stage}`
    const cerebrumImageOdpBucketName = `${process.env.CEREBRUM_IMAGE_ODP_BUCKET_NAME}${bucketStage}`
    const cerebrumImageZipBucketName = `${process.env.CEREBRUM_IMAGE_ZIP_BUCKET_NAME}${bucketStage}`

    // Buckets
    const cerebrumImageOdpBucket = new Bucket(this, cerebrumImageOdpBucketName, {
      s3Bucket: {
        bucketName: cerebrumImageOdpBucketName
      }
    })

    cerebrumImageOdpBucket.s3Bucket.addToResourcePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        principals: [new iam.ArnPrincipal(process.env.HANDLE_CEREBRUM_IMAGE_TRANSFER_ROLE_ARN as string)],
        actions: ['s3:PutObject'],
        resources: [`${cerebrumImageOdpBucket.bucketArn}/*`]
      }))

    const cerebrumImageZipBucket = new Bucket(this, cerebrumImageZipBucketName, {
      s3Bucket: {
        bucketName: cerebrumImageZipBucketName
      }
    })
    // Grant fulfillment Lambda perms to put and get Zip file in
    // destination bucket. The Get is needed to allow for the
    // signed URL download
    cerebrumImageZipBucket.s3Bucket.addToResourcePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        principals: [new iam.ArnPrincipal(process.env.HANDLE_CEREBRUM_IMAGE_FULFILLMENT_ROLE_ARN as string)],
        actions: ['s3:GetObject', 's3:PutObject'],
        resources: [`${cerebrumImageZipBucket.bucketArn}/*`]
      }))

    cerebrumImageOdpBucket.s3Bucket.addToResourcePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        principals: [new iam.ArnPrincipal(process.env.HANDLE_CEREBRUM_IMAGE_FULFILLMENT_ROLE_ARN as string)],
        actions: ['s3:GetObject'],
        resources: [`${cerebrumImageOdpBucket.bucketArn}/*`]
      }))

    // The ListObject policy is needed because the Zip operation needs to list out
    // contents of folders in order to build final list of objects to Zip
    cerebrumImageOdpBucket.s3Bucket.addToResourcePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        principals: [new iam.ArnPrincipal(process.env.HANDLE_CEREBRUM_IMAGE_FULFILLMENT_ROLE_ARN as string)],
        actions: ['s3:ListBucket'],
        resources: [`${cerebrumImageOdpBucket.bucketArn}`]
      }))
  }
}
