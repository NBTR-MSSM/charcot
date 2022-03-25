import { APIGatewayProxyEventQueryStringParameters, APIGatewayProxyEventV2, APIGatewayProxyHandlerV2 } from 'aws-lambda'
import { dynamoDbClient, HttpResponse, lambdaWrapper } from '@exsoinn/aws-sdk-wrappers'
import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client'
import { paramCase } from 'change-case'
import RangeMap from '../common/range-map'
import { Range } from '../types/charcot.types'

const indexedAttributes = ['region', 'sex', 'stain', 'age', 'race']

const validateRequest = (queryStringParameters: APIGatewayProxyEventQueryStringParameters | undefined): queryStringParameters is APIGatewayProxyEventQueryStringParameters => {
  if (!queryStringParameters) {
    return false
  }

  // Validate that at least one recognized search criteria was passed in query string
  for (const attr of indexedAttributes) {
    if (queryStringParameters[attr]) {
      return true
    }
  }
  return false
}

const addFilter = (event: APIGatewayProxyEventV2, params: DocumentClient.QueryInput) => {
  const filter = event.queryStringParameters && event.queryStringParameters.filter
  if (!filter) {
    return
  }

  const exprAttrValues: Record<string, string | number> = {}
  let dynamoFilter = filter

  /*
   * Deal with the numeric range categories (E.g. age)
   */
  // First deal with the lower and upper bounds
  for (const m of filter.matchAll(/(\w+)\s=\s(?:'(\d+)\s<='|'<\s(\d+)')/g)) {
    const num = m[2] || m[3]
    const searchStr = m[2] ? `${num} <=` : `< ${num}`
    const replaceStr = m[2] ? `${m[1]} > :${num}` : `${m[1]} <= :${num}`
    exprAttrValues[`:${num}`] = Number.parseInt(num)
    dynamoFilter = dynamoFilter.replace(searchStr, replaceStr)
  }

  // Now deal with the ranges in between
  for (const m of filter.matchAll(/(\w+)\s=\s'(\d+)\s-\s(\d+)'/g)) {
    exprAttrValues[`:${m[2]}`] = Number.parseInt(m[2])
    exprAttrValues[`:${m[3]}`] = Number.parseInt(m[3])
    dynamoFilter = dynamoFilter.replace(m[0], `${m[1]} BETWEEN :${m[2]} AND :${m[3]}`)
  }

  // Deal with the text categories (E.g. stain, region, sex, race)
  for (const m of dynamoFilter.matchAll(/(\w+)\s=\s'(\w+)'/g)) {
    exprAttrValues[`:${m[2]}`] = m[2]
    dynamoFilter = dynamoFilter.replace(`'${m[2]}'`, `:${m[2]}`)
  }

  params.FilterExpression = dynamoFilter
  params.ExpressionAttributeValues = exprAttrValues
  console.log(`JMQ: adding filter ${event.queryStringParameters && event.queryStringParameters.filter}`)
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

    // Build the search criteria. The first dimension/facet specified in the query
    // string will be the index we search.
    for (const attr of indexedAttributes) {
      let attrVal
      if (!(attrVal = queryStringParams[attr])) {
        continue
      }
      if (!indexName) {
        indexName = attr
        indexVal = `${queryStringParams[attr]}`
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

    // DynamoDB SDK requires exp attr name and value objects. This is
    // nothing more than a "lookup table" for the attribute values.
    const attrExpNames: Record<string, string> = {}
    const attrExpValues: Record<string, string> = {}
    attrExpNames[`#${indexName}`] = indexName as string
    attrExpValues[`:${indexName}`] = indexVal as string

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
  const attrExpNames: Record<string, string> = {}

  // Make dimension singular
  dimension = dimension?.substring(0, dimension?.length - 1)

  attrExpNames['#dimension'] = dimension as string
  const params: DocumentClient.QueryInput = {
    ExpressionAttributeNames: attrExpNames,
    ProjectionExpression: '#dimension',
    TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string,
    IndexName: `${dimension}Index`
  }
  addFilter(event, params)
  console.log(`JMQ: params is ${JSON.stringify(params)}`)
  const res = await dynamoDbClient.scan(params)

  let responseCode = 200
  type Dimension = { value: string | number, title: string, count: number, range: Range | undefined, rangeIndex: number }
  let ret: Dimension[] = []
  if (!res.Items) {
    responseCode = 404
  } else {
    // Ranging only applies to dimensions that are numeric
    // in nature only. Yet we do this here for all.
    const interval = Number.parseInt((event.queryStringParameters && event.queryStringParameters.interval) || '10')
    const max = Number.parseInt((event.queryStringParameters && event.queryStringParameters.max) || '90')
    const start = Number.parseInt((event.queryStringParameters && event.queryStringParameters.start) || interval.toString())
    const ranges: RangeMap = new RangeMap(interval, max, start)
    ret = Array.from(res.Items.reduce((prev: Map<string, Dimension>, cur: DocumentClient.AttributeMap) => {
      const val = Number.isInteger(cur[dimension as string]) ? cur[dimension as string] : paramCase(cur[dimension as string])
      let obj: Dimension | undefined
      if (!(obj = prev.get(val))) {
        obj = {
          count: 0,
          title: cur[dimension as string],
          value: val,
          range: undefined,
          rangeIndex: -1
        }
        prev.set(val, obj as Dimension)

        // Ranging applies to dimensions numeric in nature only (E.g. Age)
        if (Number.isInteger(val)) {
          const rangeInfo = ranges.get(val)
          obj.range = rangeInfo?.range
          obj.rangeIndex = rangeInfo?.index as number
        }
      }
      ++obj.count
      return prev
    }, new Map<string, Dimension>()).values()).map(e => e).sort((a: Dimension, b: Dimension): number => b.rangeIndex - a.rangeIndex || (a.value as unknown as number) - (b.value as unknown as number))
  }

  return new HttpResponse(responseCode, '', {
    body: ret
  })
})
