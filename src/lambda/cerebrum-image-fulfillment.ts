import { dynamoDbClient, lambdaWrapper, s3Client, sesClient } from '@exsoinn/aws-sdk-wrappers'
import { CerebrumImageOrder } from '../types/charcot.types'
import { SES } from 'aws-sdk'
import { PromiseResult } from 'aws-sdk/lib/request'
import { AWSError } from 'aws-sdk/lib/core'
import { Handler } from 'aws-lambda'

const buildZipName = (orderId: string): string => {
  return `${orderId}.zip`
}

const sendMail = async (email: string, zipPath: string): Promise<PromiseResult<SES.SendEmailResponse, AWSError>> => {
  const zipLink = await generateSignedZipLink({
    bucket: process.env.CEREBRUM_IMAGE_ZIP_BUCKET_NAME,
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
    Source: process.env.FROM_EMAIL as string
  }
  console.info(`Sending Zip to '${email}'`)
  return sesClient.sendEmail(emailParams).promise()
}

const generateSignedZipLink = ({ bucket, path, expires }: Record<string, any>): Promise<string> => {
  return s3Client.getSignedUrlPromise('getObject', { Bucket: bucket, Key: path, Expires: expires })
}

export const handle: Handler = lambdaWrapper(async ({ orderId }: Record<string, any>) => {
  /*
  * Alg:
  * The orderId will already contain the file names, so:
  * 1. DONE Pull the files from S3
  * 2. DONE Add the files to a Zip
  * 3. DONE Put the Zip in target S3 (can augment s3-client to support this op, 'retrieveObjectsAsZip')
  * 4. DONE Send email to requestor
  * 5. TODO Write record to DynamoDB of what happened (who/what/when, etc.)
  */
  let order
  try {
    const zipName = buildZipName(orderId)
    const zipBucket = process.env.CEREBRUM_IMAGE_ZIP_BUCKET_NAME
    console.log(`Working on creating a Zip '${zipBucket}/zip/${zipName}' for order ${orderId}`)
    const dbResult = await dynamoDbClient.get({
      TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME,
      Key: {
        orderId: orderId
      }
    })
    order = dbResult.Item as unknown as CerebrumImageOrder
    const zipPath = `zip/${zipName}`
    // Include the corresponding folder for the image also, E.g. fileName=[ '480654_1_Tau_1.mrxs' ], below
    // produces [ '480654_1_Tau_1.mrxs', '480654_1_Tau_1/' ]
    const s3Objects = order.fileNames.map(e => [e, e.replace(/\..+$/, '/')]).flat()
    console.info(`The following assets will be zipped up, ${JSON.stringify(s3Objects)}`)
    await (s3Client as any).zipObjectsToBucket(process.env.CEREBRUM_IMAGE_ODP_BUCKET_NAME, '', s3Objects, zipBucket, zipPath)
    console.info(`Successfully created Zip for request ${JSON.stringify(orderId)}`)
    // DONE: Must include the folder also of the mrsx image data
    // DONE: Send email that Zip is ready, with the link download
    // TODO: Update order with fulfillment date, time it took to process, etc.
    await sendMail(order.email, zipPath)
  } catch (e) {
    console.error(`Problem processing order ${JSON.stringify(order)}`, e)
  }
})
