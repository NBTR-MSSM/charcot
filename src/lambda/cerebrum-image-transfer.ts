import { S3Handler } from 'aws-lambda'
import { lambdaWrapper, s3Client } from '@exsoinn/aws-sdk-wrappers'
import { S3Event } from 'aws-lambda/trigger/s3'

const flexiS3Client = s3Client as any

export const handle: S3Handler = lambdaWrapper(async (event: S3Event) => {
  console.info(JSON.stringify(event))
  for (const rec of event.Records) {
    const fromBucket = rec.s3.bucket.name
    const fromPath = rec.s3.object.key
    const toPath = fromPath
    await flexiS3Client.copy(fromBucket, fromPath, process.env.CEREBRUM_IMAGE_ODP_BUCKET_NAME as string, toPath)
  }
})
