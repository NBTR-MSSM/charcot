import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client'
import { dynamoDbClient, HttpResponse } from '@exsoinn/aws-sdk-wrappers'
import { Range } from '../types/charcot.types'
import RangeMap from '../common/range-map'
import { paramCase } from 'change-case'
import { APIGatewayProxyEventV2, APIGatewayProxyStructuredResultV2 } from 'aws-lambda'

class ImageSearch {
  async search(event: APIGatewayProxyEventV2): Promise<APIGatewayProxyStructuredResultV2> {
    const params: DocumentClient.QueryInput = {
      TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string
    }
    this.addFilter(event, params)
    const res = await dynamoDbClient.scan(params)
    let responseCode = 200
    if (!res.Items) {
      responseCode = 404
    }
    return new HttpResponse(responseCode, '', {
      body: res.Items
    })
  }

  async dimension(event: APIGatewayProxyEventV2) {
    let dimension = (event.pathParameters && event.pathParameters.dimension) as string

    const attrExpNames: Record<string, string> = {}

    // Make dimension singular
    dimension = dimension.substring(0, dimension?.length - 1)

    attrExpNames['#dimension'] = dimension
    const params: DocumentClient.QueryInput = {
      ExpressionAttributeNames: attrExpNames,
      ProjectionExpression: '#dimension',
      TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string,
      IndexName: `${dimension}Index`
    }
    this.addFilter(event, params)
    console.log(`JMQ: params is ${JSON.stringify(params)}`)
    const res = await dynamoDbClient.scan(params)

    let responseCode = 200
    type Dimension = { value: string | number, title: string, count: number, range: Range | undefined, rangeIndex: number }
    let ret: Dimension[] = []
    if (!res.Items) {
      responseCode = 404
    } else {
      // Ranging only applies to dimensions that are numeric
      // in nature only. Yet we do this here for all for sake of simplicity,
      // namely generating a RangeMap needlessly if the dimension in question
      // is not numeric in nature.
      const interval = Number.parseInt((event.queryStringParameters && event.queryStringParameters.interval) || '10')
      const max = Number.parseInt((event.queryStringParameters && event.queryStringParameters.max) || '90')
      const start = Number.parseInt((event.queryStringParameters && event.queryStringParameters.start) || interval.toString())
      const ranges: RangeMap = new RangeMap(interval, max, start)

      ret = Array.from(res.Items.reduce((prev: Map<string, Dimension>, cur: DocumentClient.AttributeMap) => {
        const val = Number.isInteger(cur[dimension]) ? cur[dimension] : paramCase(cur[dimension])
        let obj: Dimension | undefined
        if (!(obj = prev.get(val))) {
          obj = {
            count: 0,
            title: cur[dimension],
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
  }

  /**
   * Augments the passed in DynamoDB query with filter found in the
   * request query string, if any. Otherwise it leaves the DynamoDB query
   * untouched.v
   */
  private addFilter(event: APIGatewayProxyEventV2, params: DocumentClient.QueryInput) {
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
}

export default new ImageSearch()
