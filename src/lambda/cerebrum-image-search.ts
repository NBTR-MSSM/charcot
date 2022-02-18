import { APIGatewayProxyEventQueryStringParameters, APIGatewayProxyEventV2, APIGatewayProxyHandlerV2 } from 'aws-lambda'
import { dynamoDbClient, HttpResponse, lambdaWrapper } from '@exsoinn/aws-sdk-wrappers'
import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client'
import { paramCase } from 'change-case'

const indexedAttributes = ['region', 'sex', 'stain', 'age', 'race']

const validateRequest = (queryStringParameters: APIGatewayProxyEventQueryStringParameters | undefined): queryStringParameters is APIGatewayProxyEventQueryStringParameters => {
  if (!queryStringParameters) {
    return false
  }

  // Validate thast at least one recognized search criterium was passed in query string
  for (const attr of indexedAttributes) {
    if (queryStringParameters[attr]) {
      return true
    }
  }
  return false
}

export const search: APIGatewayProxyHandlerV2 = lambdaWrapper(async (event: APIGatewayProxyEventV2) => {
  // DONE: Create GSI's (Global Indexes) as per these SO questions,
  //  [REF|https://stackoverflow.com/questions/47569793/how-do-i-query-dynamodb-with-non-primary-key-field|"AWS Console > DynamoDb > tab Indexes of your table > Create index >"],
  //  and [REF|https://stackoverflow.com/questions/43353852/query-on-non-key-attribute|"You'll need to set up a global secondary index (GSI)"]

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
    // TODO: User authentication (user/passwd?, federated via AD?)

    const params: DocumentClient.QueryInput = {
      TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string,
      IndexName: `${indexName}Index`
    }

    // DynamoDB SDK requires exp attr name and values objects. This is
    // nothing more than a "lookup table" for the attribute values.
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
    // fileNames: queryRes?.Items?.map((i: Record<string, any>) => i.fileName) as string[]
    const queryRes = await dynamoDbClient.query(params)
    return new HttpResponse(200, '', {
      body: queryRes.Items
    })
  } catch (e) {
    return new HttpResponse(500, `Something went wrong, ${e}`)
  }
})

// TODO: Unit test me
export const dimension: APIGatewayProxyHandlerV2 = lambdaWrapper(async (event: APIGatewayProxyEventV2) => {
  let dimension = event.pathParameters && event.pathParameters.dimension
  const attrExpNames: Record<string, any> = {}

  // Make dimension singular
  dimension = dimension?.substring(0, dimension?.length - 1)

  attrExpNames[`#${dimension}`] = dimension
  const res = await dynamoDbClient.scan({
    ExpressionAttributeNames: attrExpNames,
    ProjectionExpression: `#${dimension}`,
    TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string
  })

  let responseCode = 200
  let ret: { value: string, title: string }[] = []
  if (!res.Items) {
    responseCode = 404
  } else {
    ret = Array.from(new Map(res.Items.map(e => [e[dimension as string], e[dimension as string]])).values()).map(e => {
      return {
        value: paramCase(e),
        title: e
      }
    })
  }

  return new HttpResponse(responseCode, '', {
    body: ret
  })
})
