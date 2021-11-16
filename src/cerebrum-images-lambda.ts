import { APIGatewayProxyEventV2, APIGatewayProxyHandlerV2 } from 'aws-lambda'
import { CerebrumImageMetaData, CerebrumImageMetaDataCreateResult } from './types/charcot.types'
import { dynamoDbClient, HttpResponse, lambdaWrapper } from '@exsoinn/aws-sdk-wrappers'

export const create: APIGatewayProxyHandlerV2 = lambdaWrapper(async (event: APIGatewayProxyEventV2) => {
  /*
   * TODO: Parse the request parameters and insert the record into CerebrumImageMetaData DynamoDB table
   */
  const payload: CerebrumImageMetaData[] = JSON.parse(event.body as string)
  const promises: Promise<CerebrumImageMetaDataCreateResult>[] = []
  for (const img of payload) {
    const params = {
      TableName: process.env.TABLE_NAME,
      Item: img
    }
    // Fail-safe, will report on what failed and what was successful, but as a whole
    // the HTTP response will always be a successful one
    const res: CerebrumImageMetaDataCreateResult = {
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

  let results: CerebrumImageMetaDataCreateResult[] = []
  await Promise.all(promises).then((val) => {
    results = val
  })

  return new HttpResponse(200, 'Request processed, failures if any are indicated', {
    results: results
  })
})
