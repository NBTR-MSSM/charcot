import { APIGatewayProxyEventV2, APIGatewayProxyHandlerV2 } from 'aws-lambda'
import { CerebellumImageMetaData, CerebellumImageMetaDataCreateResult } from './types/charcot.types'
import { dynamoDbClient, HttpResponse, lambdaWrapper } from '@exsoinn/aws-sdk-wrappers'

export const create: APIGatewayProxyHandlerV2 = lambdaWrapper(async (event: APIGatewayProxyEventV2) => {
  /*
   * TODO: Parse the request parameters and insert the record into CerebellumImageMetaData DynamoDB table
   */
  const payload: CerebellumImageMetaData[] = JSON.parse(event.body as string)
  const promises: Promise<CerebellumImageMetaDataCreateResult>[] = []
  for (const img of payload) {
    const params = {
      TableName: process.env.TABLE_NAME,
      Item: img
    }
    // Fail-safe, will report on what failed and what was successful, but as a whole
    // the HTTP response will always be a successful one
    const res: CerebellumImageMetaDataCreateResult = {
      image: img,
      success: true,
      message: 'Successfully created'
    }

    promises.push(dynamoDbClient.put(params).then(() => res).catch((e) => {
      res.message = e.message
      res.success = false
      return res
    }))
  }

  let results: CerebellumImageMetaDataCreateResult[] = []
  await Promise.all(promises).then((val) => {
    results = val
  })

  return new HttpResponse(200, 'Request processed, failures if any are indicated', {
    results: results
  })
})
