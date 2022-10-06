import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client'
import { HttpResponse } from '@exsoinn/aws-sdk-wrappers'
import { Dimension, Filter } from '../types/charcot.types'
import RangeMap from '../common/range-map'
import { paramCase } from 'change-case'
import { APIGatewayProxyEventV2 } from 'aws-lambda'
import { singular } from 'pluralize'
import { rank } from '../common/rank'
import Search from './search'

class ImageSearch extends Search {
  async search(filter: Filter): Promise<Record<string, any>> {
    const params: DocumentClient.QueryInput = {
      TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string
    }
    this.addFilter(filter, params)
    console.log(`JMQ: search() params is ${JSON.stringify(params)}`)
    let responseCode = 404
    let retItems: DocumentClient.ItemList = []
    const callback = (items: DocumentClient.ItemList) => {
      retItems = retItems.concat(items)
      responseCode = 200
    }
    await this.handleSearch(params, callback)
    return new HttpResponse(responseCode, '', {
      body: retItems
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
    this.addFilter((event.queryStringParameters && event.queryStringParameters.filter) as Filter, params)
    this.addEnabledOnlyCondition(params)
    console.log(`JMQ: dimension() params is ${JSON.stringify(params)}`)

    let ret: Dimension[] = []
    let responseCode = 404

    const callback = (items: DocumentClient.ItemList) => {
      responseCode = 200
      // Ranging only applies to dimensions that are numeric
      // in nature only. Yet we do this here for all for sake of simplicity,
      // namely generating a RangeMap needlessly if the dimension in question
      // is not numeric in nature.
      const interval = Number.parseInt((event.queryStringParameters && event.queryStringParameters.interval) || '10')
      const max = Number.parseInt((event.queryStringParameters && event.queryStringParameters.max) || '90')
      const start = Number.parseInt((event.queryStringParameters && event.queryStringParameters.start) || interval.toString())
      const ranges: RangeMap = new RangeMap(interval, max, start)
      ret = Array.from(items.reduce((prev: Map<string | number, Dimension>, cur: DocumentClient.AttributeMap) => {
        const val = Number.isInteger(cur[dimension]) && isNumeric ? cur[dimension] : paramCase(`${cur[dimension]}`)
        let obj: Dimension | undefined
        if (!(obj = prev.get(val))) {
          obj = {
            count: 0,
            title: cur[dimension],
            value: val,
            range: undefined,
            rank: -1
          }
          prev.set(val, obj as Dimension)

          // Ranging applies to dimensions numeric in nature only (E.g. Age) and where
          // caller indeed wants to treat those as range-able (numeric=true in query string params)
          if (Number.isInteger(val) && isNumeric) {
            const rangeInfo = ranges.get(val)
            obj.range = rangeInfo?.range
            obj.rank = rangeInfo?.rank as number
          }
        }
        ++obj.count
        return prev
      }, new Map<string | number, Dimension>(ret.map((obj) => [obj.value, obj]))).values())
        .sort((a, b): number => (b as Dimension).rank - (a as Dimension).rank || rank(dimension, (a as Dimension).title) - rank(dimension, (b as Dimension).title)) as Dimension[]
    }
    await this.handleSearch(params, callback)
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
  private addFilter(filter: Filter, params: DocumentClient.QueryInput) {
    if (!filter) {
      return
    }
    // console.log(`JMQ: raw filter is ${filter}`)

    const exprAttrNames: Record<string, string> = {}
    const exprAttrValues: Record<string, string | number> = {}
    let dynamoFilter = filter

    /*
     * Deal with the numeric range categories (E.g. age)
     */
    // First deal with the less than and greater than ranges (the bottom and top ones in the chart)
    for (const m of filter.matchAll(/((\w+)\s=\s(?:'(\d+)\+'|'<\s(\d+)'))/g)) {
      const category = m[2]
      const categoryPlaceholder = `#${category.replace(/\s+/g, '')}`
      const greaterThanOrEqualTo = m[3]
      const lt = m[4]
      const num = greaterThanOrEqualTo || lt
      const searchStr = m[1]
      const replaceStr = greaterThanOrEqualTo ? `${categoryPlaceholder} >= :${num}` : `${categoryPlaceholder} < :${num}`
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
      // Globally handle category replacement upon the first iteration and
      // upon the first iteration only. Why though? This is idempotent so can run many times
      // harmlessly, right?
      const category = m[1]
      const categoryPlaceholder = `#${category.replace(/\s+/g, '')}`
      if (!exprAttrNames[categoryPlaceholder]) {
        exprAttrNames[categoryPlaceholder] = category
        dynamoFilter = dynamoFilter.replace(new RegExp(category, 'g'), categoryPlaceholder)
      }

      const val = m[2]
      const valuePlaceHolder = `:${val.replace(/\W+/g, '')}`
      // Ensure numeric values are stored as JavaScript numeric type, else DynamoDB
      // returns results because it won't coerce to number strings that  are numeric
      // in nature
      console.log(`JMQ: val is ${val}`)
      exprAttrValues[valuePlaceHolder] = val.match(/^\d+$/) ? parseInt(val) : val.replace(/__QUOTE__/g, "'")
      dynamoFilter = dynamoFilter.replace(`'${val}'`, valuePlaceHolder)
    }

    params.FilterExpression = dynamoFilter
    params.ExpressionAttributeNames = { ...params.ExpressionAttributeNames, ...exprAttrNames }
    params.ExpressionAttributeValues = exprAttrValues
    console.log(`JMQ: exprAttrValues is ${JSON.stringify(exprAttrValues)}`)
    console.log(`JMQ: addFilter() DynamoDB filter is ${dynamoFilter}`)
  }

  private addEnabledOnlyCondition(params: DocumentClient.QueryInput) {
    let curFilter = params.FilterExpression
    curFilter = curFilter ? `${(curFilter)} AND ` : ''
    params.FilterExpression = `${curFilter}#enabled = :true`
    const attrNames = {
      '#enabled': 'enabled'
    }
    const attrVals = {
      ':true': 'true'
    }

    params.ExpressionAttributeNames = { ...params.ExpressionAttributeNames, ...attrNames }
    params.ExpressionAttributeValues = { ...params.ExpressionAttributeValues, ...attrVals }

    console.log(`JMQ: addEnabledOnlyCondition() DynamoDB filter is ${params.FilterExpression}`)
  }
}

export default new ImageSearch()
