import { APIGatewayProxyEventQueryStringParameters, APIGatewayProxyEventV2, APIGatewayProxyHandlerV2 } from 'aws-lambda'
import { dynamoDbClient, HttpResponse, lambdaClient, lambdaWrapper } from '@exsoinn/aws-sdk-wrappers'
import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client'
import { CerebrumImageRequest } from '../types/charcot.types'

const indexedAttributes = ['regionName', 'sex', 'stain', 'age', 'race']

const validateRequest = (queryStringParameters: APIGatewayProxyEventQueryStringParameters | undefined): queryStringParameters is APIGatewayProxyEventQueryStringParameters => {
  if (!queryStringParameters || !queryStringParameters.requestorEmail) {
    return false
  }

  for (const attr of indexedAttributes) {
    if (queryStringParameters[attr]) {
      return true
    }
  }
  return false
}

export const handle: APIGatewayProxyHandlerV2 = lambdaWrapper(async (event: APIGatewayProxyEventV2) => {
  // TODO: After some back and forth between user and this Lambda,
  //   the final list of files requested is ready to be sent to processor.
  try {
    let indexName, indexVal
    const filterExpression = new Map<string, string>()
    const queryStringParams = event.queryStringParameters
    if (!validateRequest(queryStringParams)) {
      return new HttpResponse(401, `Request is invalid: ${JSON.stringify(queryStringParams)}`)
    }

    for (const attr of indexedAttributes) {
      let attrVal
      if (!(attrVal = queryStringParams[attr])) {
        continue
      }
      if (!indexName) {
        indexName = attr
        indexVal = `${queryStringParams[attr]}`
        continue
      } else {
        filterExpression.set(attr, attrVal)
      }
    }

    // TODO: Bark with HTTP 401 if unable to determine indexName at the very least.

    const params: DocumentClient.QueryInput = {
      TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string,
      IndexName: `${indexName}Index`,
      ProjectionExpression: 'fileName'
    }

    // DynamoDB requires exp attr values object, put one together. This is
    // nothing more than a "lookup table" for the attribute values
    const attrExpNames: Record<string, any> = {}
    const attrExpValues: Record<string, any> = {}
    attrExpNames[`#${indexName}`] = indexName
    attrExpValues[`:${indexName}`] = indexVal

    if (filterExpression.size > 0) {
      for (const [key, value] of filterExpression) {
        attrExpNames[`#${key}`] = key
        attrExpValues[`:${key}`] = value
      }
      params.FilterExpression = Array.from(filterExpression.keys()).map(key => `#${key} = :${key}`).join(' AND ')
    }

    params.ExpressionAttributeNames = attrExpNames
    params.ExpressionAttributeValues = attrExpValues
    params.KeyConditionExpression = `#${indexName} = :${indexName}`

    const queryRes = await dynamoDbClient.query(params)
    const lambdaName = process.env.HANDLE_CEREBRUM_IMAGE_REQUEST_FUNCTION_NAME
    const request: CerebrumImageRequest = {
      requestorEmail: queryStringParams.requestorEmail as string,
      fileNames: queryRes.Items.map((i: Record<string, any>) => i.fileName),
      created: new Date().toISOString()
    }
    const lambdaRes = await lambdaClient.invokeLambda(lambdaName, 'Event',
      JSON.stringify(request), lambdaName)
    return new HttpResponse(lambdaRes.StatusCode, 'Your request is being processed, you will get an email soon')
  } catch (e) {
    return new HttpResponse(500, `Something went wrong, ${e}`)
  }
})
