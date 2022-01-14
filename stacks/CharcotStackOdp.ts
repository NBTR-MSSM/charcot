import * as sst from '@serverless-stack/resources'
import { Bucket } from '@serverless-stack/resources'
import * as iam from '@aws-cdk/aws-iam'

export default class CharcotStackOdp extends sst.Stack {
  constructor(scope: sst.App, id: string, props?: sst.StackProps) {
    super(scope, id, props)
    {
      // Buckets
      const cerebrumImageOdpBucket = new Bucket(this, process.env.CEREBRUM_IMAGE_ODP_BUCKET_NAME as string, {
        s3Bucket: {
          bucketName: process.env.CEREBRUM_IMAGE_ODP_BUCKET_NAME
        }
      })

      // TODO: Figure out if there's way to
      //  populate Lambda role ARN dynamically by outputting it
      //  in CharcotStack
      cerebrumImageOdpBucket.s3Bucket.addToResourcePolicy(
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          principals: [new iam.ArnPrincipal('arn:aws:iam::433661183964:role/dev-charcot-charcot-stack-HandleCerebrumImageTrans-ZPQU1IO615QA')],
          actions: ['s3:PutObject'],
          resources: [`${cerebrumImageOdpBucket.bucketArn}/*`]
        }))

      const cerebrumImageZipBucketName = process.env.CEREBRUM_IMAGE_ZIP_BUCKET_NAME as string
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
          principals: [new iam.ArnPrincipal('arn:aws:iam::433661183964:role/dev-charcot-charcot-stack-HandleCerebrumImageFulfi-FC4S69T27R79')],
          actions: ['s3:GetObject', 's3:PutObject'],
          resources: [`${cerebrumImageZipBucket.bucketArn}/*`]
        }))
    }
  }
}
