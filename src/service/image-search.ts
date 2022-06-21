import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client'
import { dynamoDbClient, HttpResponse } from '@exsoinn/aws-sdk-wrappers'
import { Range } from '../types/charcot.types'
import RangeMap from '../common/range-map'
import { paramCase } from 'change-case'
import { APIGatewayProxyEventV2 } from 'aws-lambda'
import { singular } from 'pluralize'

class ImageSearch {
  async search(event: APIGatewayProxyEventV2): Promise<Record<string, any>> {
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
    const isNumeric = event.queryStringParameters && event.queryStringParameters.numeric === 'true'
    const attrExpNames: Record<string, string> = {}

    // Make dimension singular because that's how the
    // DynamoDB tables are named
    dimension = singular(dimension)

    attrExpNames['#dimension'] = dimension
    const params: DocumentClient.QueryInput = {
      ExpressionAttributeNames: attrExpNames,
      ProjectionExpression: '#dimension',
      TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string,
      IndexName: `${dimension}Index`
    }
    this.addFilter(event, params)
    console.log(`JMQ: params is ${JSON.stringify(params)}`)

    type Dimension = { value: string | number, title: string, count: number, range: Range | undefined, rangeIndex: number }
    let ret: Dimension[] = []
    let responseCode = 404
    while (true) {
      const res = await dynamoDbClient.scan(params)
      const items = res.Items
      // console.log(`JMQ: items is ${JSON.stringify(items)}`)
      // If dynamo returned 0 items and this is first iteratioon of the loop, return
      // HTTP not found code (404)
      if (!items) {
        break
      } else {
        responseCode = 200
        // Ranging only applies to dimensions that are numeric
        // in nature only. Yet we do this here for all for sake of simplicity,
        // namely generating a RangeMap needlessly if the dimension in question
        // is not numeric in nature.
        const interval = Number.parseInt((event.queryStringParameters && event.queryStringParameters.interval) || '10')
        const max = Number.parseInt((event.queryStringParameters && event.queryStringParameters.max) || '90')
        const start = Number.parseInt((event.queryStringParameters && event.queryStringParameters.start) || interval.toString())
        const ranges: RangeMap = new RangeMap(interval, max, start)
        // console.log(`JMQ: LastEvaluatedKey is ${JSON.stringify(res.LastEvaluatedKey)}`)
        ret = Array.from(items.reduce((prev: Map<string | number, Dimension>, cur: DocumentClient.AttributeMap) => {
          // console.log(`JMQ: cur is ${JSON.stringify(cur)}`)
          const val = Number.isInteger(cur[dimension]) && isNumeric ? cur[dimension] : paramCase(`${cur[dimension]}`)
          // console.log(`JMQ: val is ${cur[dimension]}`)
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
            if (Number.isInteger(val) && isNumeric) {
              const rangeInfo = ranges.get(val)
              obj.range = rangeInfo?.range
              obj.rangeIndex = rangeInfo?.index as number
            }
          }
          ++obj.count
          return prev
        }, new Map<string | number, Dimension>(ret.map((obj) => [obj.value, obj]))).values())
          .sort((a: Dimension, b: Dimension): number => b.rangeIndex - a.rangeIndex || (a.value as unknown as number) - (b.value as unknown as number))
      }

      const lastEvaluatedKey = res.LastEvaluatedKey
      if (lastEvaluatedKey) {
        // console.log(`JMQ: lastEvaluatedKey is ${JSON.stringify(lastEvaluatedKey)}`)
        params.ExclusiveStartKey = lastEvaluatedKey
      } else {
        break
      }
    }

    return new HttpResponse(responseCode, '', {
      body: ret
    })
  }

  /**
   * Augments the passed in DynamoDB query with the string filter found in the
   * request query string, if any, converting it to a DynamoDB filter. Otherwise it leaves
   * the DynamoDB query untouched.
   * WARNING: Fairly heavy use of RegEx alert.
   */
  private addFilter(event: APIGatewayProxyEventV2, params: DocumentClient.QueryInput) {
    const filter = event.queryStringParameters && event.queryStringParameters.filter
    if (!filter) {
      return
    }
    console.log(`JMQ: raw filter is ${filter}`)

    const exprAttrNames: Record<string, string> = {}
    const exprAttrValues: Record<string, string | number> = {}
    let dynamoFilter = filter

    /*
     * Deal with the numeric range categories (E.g. age)
     */
    // First deal with the less than and greater than ranges (the bottom and top ones in the chart)
    for (const m of filter.matchAll(/((\w+)\s=\s(?:'(\d+)\s<='|'<\s(\d+)'))/g)) {
      const category = m[2]
      const categoryPlaceholder = `#${category.replace(/\s+/g, '')}`
      const gtOrEqTo = m[3]
      const lt = m[4]
      const num = gtOrEqTo || lt
      const searchStr = m[1]
      const replaceStr = gtOrEqTo ? `${categoryPlaceholder} > :${num}` : `${categoryPlaceholder} <= :${num}`
      exprAttrNames[categoryPlaceholder] = category
      exprAttrValues[`:${num}`] = Number.parseInt(num)
      dynamoFilter = dynamoFilter.replace(searchStr, replaceStr)
    }

    // Now deal with the ranges in between
    for (const m of filter.matchAll(/(\w+)\s=\s'(\d+)\s-\s(\d+)'/g)) {
      const category = m[1]
      const categoryPlaceholder = `#${category.replace(/\s+/g, '')}`
      const from = m[2]
      const to = m[3]
      exprAttrNames[categoryPlaceholder] = category
      exprAttrValues[`:${from}`] = Number.parseInt(from)
      exprAttrValues[`:${to}`] = Number.parseInt(to)
      dynamoFilter = dynamoFilter.replace(m[0], `${categoryPlaceholder} BETWEEN :${from} AND :${to}`)
    }

    // Deal with the text categories (E.g. stain, region, sex, race, diagnosis)
    for (const m of dynamoFilter.matchAll(/(\w+)\s=\s'([^']+)'/g)) {
      // Handle category replacement one time only, in the first iteration
      const category = m[1]
      const categoryPlaceholder = `#${category.replace(/\s+/g, '')}`
      if (!exprAttrNames[categoryPlaceholder]) {
        exprAttrNames[categoryPlaceholder] = category
        dynamoFilter = dynamoFilter.replace(new RegExp(category, 'g'), categoryPlaceholder)
      }

      const val = m[2]
      const valuePlaceHolder = `:${val.replace(/\W+/g, '')}`
      exprAttrValues[valuePlaceHolder] = val.replace(/__QUOTE__/g, "'")
      dynamoFilter = dynamoFilter.replace(`'${val}'`, valuePlaceHolder)
    }

    params.FilterExpression = dynamoFilter
    params.ExpressionAttributeNames = { ...params.ExpressionAttributeNames || {}, ...exprAttrNames }
    params.ExpressionAttributeValues = exprAttrValues
    console.log(`JMQ: DynamoDB filter is ${dynamoFilter}`)
  }
}

export default new ImageSearch()
