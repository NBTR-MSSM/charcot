import { dynamoDbClient, lambdaWrapper, s3Client, sesClient } from '@exsoinn/aws-sdk-wrappers'
import { CerebrumImageOrder } from '../types/charcot.types'
import { S3, SES } from 'aws-sdk'
import { PromiseResult } from 'aws-sdk/lib/request'
import { AWSError } from 'aws-sdk/lib/core'

const buildZipName = (orderId: string): string => {
  return `${orderId}.zip`
}

// TODO: Create new type for the email args?
const sendMail = async (email: string, zipPath: string): Promise<PromiseResult<SES.SendEmailResponse, AWSError>> => {
  const zipLink = await generateSignedZipLink({
    bucket: process.env.CEREBRUM_IMAGE_ZIP_BUCKET_NAME as string,
    path: zipPath,
    expires: process.env.ZIP_LINK_EXPIRY as string
  })
  const emailParams = {
    Destination: {
      ToAddresses: [email]
    },
    Message: {
      Body: {
        Text: { Data: `Your requested image Zip is ready. You can access via this link, ${zipLink}.` }
      },
      Subject: { Data: 'Charcot Image Request Ready' }
    },
    Source: 'joquijada2010@gmail.com'
  }
  return (sesClient as SES).sendEmail(emailParams).promise()
}

const generateSignedZipLink = ({ bucket, path, expires }: Record<string, any>): Promise<string> => {
  return (s3Client as unknown as S3).getSignedUrlPromise('getObject', { Bucket: bucket, Key: path, Expires: expires })
}

export const handle = lambdaWrapper(async (orderId: string) => {
  /*
  * Alg:
  * The orderId will already contain the file names, so:
  * 1. DONE Pull the files from S3
  * 2. DONE Add the files to a Zip
  * 3. DONE Put the Zip in target S3 (can augment s3-client to support this op, 'retrieveObjectsAsZip')
  * 4. DONE Send email to requestor
  * 5. TODO Write record to DynamoDB of what happened (who/what/when, etc.)
  */
  try {
    const zipName = buildZipName(orderId)
    const zipBucket = process.env.CEREBRUM_IMAGE_ZIP_BUCKET_NAME
    console.log(`Working on creating a Zip '${zipBucket}/zip/${zipName}' for request ${JSON.stringify(orderId)}`)
    const dbResult = await dynamoDbClient.get({
      TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME,
      Key: {
        orderId: orderId
      }
    })
    const order = dbResult.Item as unknown as CerebrumImageOrder
    await s3Client.zipObjectsToBucket(process.env.CEREBRUM_IMAGE_BUCKET_NAME, 'image/',
      order.fileNames, zipBucket, `zip/${zipName}`)
    console.log(`Successfully created Zip for request ${JSON.stringify(orderId)}`)
    // TODO: Must include the folder also of the mrsx image data
    // DONE: Send email that Zip is ready, with the link download
    // TODO: Write transaction record to table (who, when it was requested, when it was fulfilled)
    await sendMail(order.email, `${zipBucket}/${zipName}`)
  } catch (e) {
    console.error(`Problem processing request ${JSON.stringify(orderId)}`, e)
  }
})
