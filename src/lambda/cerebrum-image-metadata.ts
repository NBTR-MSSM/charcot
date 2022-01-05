import { APIGatewayProxyEventV2, APIGatewayProxyHandlerV2 } from 'aws-lambda'
import { CerebrumImageMetaData } from '../types/charcot.types'
import { dynamoDbClient, HttpResponse, lambdaWrapper } from '@exsoinn/aws-sdk-wrappers'
import { PromiseResult } from 'aws-sdk/lib/request'
import { AWSError } from 'aws-sdk/lib/core'
import { DynamoDB } from 'aws-sdk/clients/all'

/**
 * Accepts requests to insert image metadata into table. Once the last image is received, in A/B switch
 * fashion copies the data to the currently active table
 */
export const create: APIGatewayProxyHandlerV2 = lambdaWrapper(async (event: APIGatewayProxyEventV2) => {
  /*
   * TODO: Parse the request parameters and insert the record into CerebrumImageMetaData DynamoDB table
   */
  // Identify table that is ready next for receiving this data
  const payload: CerebrumImageMetaData[] = JSON.parse(event.body as string)
  const promises: Promise<PromiseResult<DynamoDB.DocumentClient.PutItemOutput, AWSError>>[] = []
  let endOfSnapshot = false
  for (const img of sortByImageNumber(payload)) {
    if (img.imageNumber === img.total) {
      // When the current image number matches the total,
      // it means we reached the end of the snapshot
      // being sent by the client
      endOfSnapshot = true
    }

    const params = {
      TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME,
      Item: img
    }

    promises.push(dynamoDbClient.put(params))
  }

  const response = new HttpResponse(200, 'Request processed successfully')
  await Promise.all(promises).catch(e => {
    response.statusCode = 400
    response.message = `Problem processing image data: ${e}`
  })

  if (endOfSnapshot) {
    // TODO: Since Joshua will be sending a full snapshot
    //       every time, see which records were not seen in
    //       this snapshot, and mark them as "inactive". This
    //       is the same approach I followed with TM's inventory
    //       updates from POS.
  }

  return response
})

/**
 * Helper method to ensure that images are handled sorted by imageNumber. Why? Because
 * Joshua will send a snapshot split into chunks, and we want to handle things sequentially
 * so that Joshua can pick up from where he left off in case of failure (I.e. no need to
 * redo the full snapshot from scratch).
 */
const sortByImageNumber = (imageMetadata: CerebrumImageMetaData[]): CerebrumImageMetaData[] => {
  return imageMetadata.sort((a: CerebrumImageMetaData, b: CerebrumImageMetaData): number => a.imageNumber - b.imageNumber)
}
