import { lambdaWrapper, s3Client } from '@exsoinn/aws-sdk-wrappers'
import { CerebrumImageRequest } from '../types/charcot.types'

const buildZipName = (request: CerebrumImageRequest): string => {
  const isoDate = new Date().toISOString().slice(0, 19).replace(/T|:/g, '-')
  const email = request.requestorEmail.toLowerCase().replace(/\W/g, '-')
  return `${email}-${isoDate}.zip`
}
export const handle = lambdaWrapper(async (request: CerebrumImageRequest) => {
  // TODO: Create GSI's (Global Indexes) as per these SO questions,
  //  [REF|https://stackoverflow.com/questions/47569793/how-do-i-query-dynamodb-with-non-primary-key-field|"AWS Console > DynamoDb > tab Indexes of your table > Create index >"],
  //  and [REF|https://stackoverflow.com/questions/43353852/query-on-non-key-attribute|""You'll need to set up a global secondary index (GSI)]
  /*
   * Alg:
   * The request will already contain the file names, so:
   * 1. DONE Pull the files from S3
   * 2. DONE Add the files to a Zip
   * 3. DONE Put the Zip in target S3 (can augment s3-client to support this op, 'retrieveObjectsAsZip')
   * 4. TODO Send email to requestor
   * 5. TODO Write record to DynamoDB of what happened (who/what/when, etc.)
   */
  try {
    const zipName = buildZipName(request)
    const zipBucket = process.env.CEREBRUM_IMAGE_ZIP_BUCKET_NAME
    console.log(`Working on creating a Zip '${zipBucket}/zip/${zipName}' for request ${JSON.stringify(request)}`)
    await s3Client.zipObjectsToBucket(process.env.CEREBRUM_IMAGE_BUCKET_NAME, 'image/',
      request.fileNames, zipBucket, `zip/${zipName}`)
    console.log(`Successfully created Zip for request ${JSON.stringify(request)}`)
    // TODO: Must include the folder also of the mrsx image data
    // TODO: Send email that Zip is ready, with the link download
    // TODO: Write transaction record to table
  } catch (e) {
    console.error(`Problem processing request ${JSON.stringify(request)}`, e)
  }
})
