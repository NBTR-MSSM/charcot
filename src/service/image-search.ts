import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client'
import { HttpResponse } from '@exsoinn/aws-sdk-wrappers'
import { Dimension, Filter } from '../types/charcot.types'
import RangeMap from '../common/range-map'
import { paramCase } from 'change-case'
import { APIGatewayProxyEventV2 } from 'aws-lambda'
import { singular } from 'pluralize'
import { rank } from '../common/rank'
import Search from './search'
import Pagination from './pagination'

const isFilter = (input: unknown): input is Filter => {
  return typeof input === 'string'
}

interface PrepareCallbackArgs {
  dimension: string
  event: APIGatewayProxyEventV2
  isNumeric?: boolean
  results?: Dimension[]
}

class ImageSearch extends Search {
  async search(event: APIGatewayProxyEventV2 | Filter): Promise<Record<string, unknown>> {
    const params: DocumentClient.QueryInput = {
      TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string
    }

    const filter: Filter = isFilter(event) ? event : (event.queryStringParameters && event.queryStringParameters.filter) as string
    this.addFilter(filter, params)
    let retItems: DocumentClient.ItemList = []
    const callback = (scanOutput: DocumentClient.ScanOutput, items: DocumentClient.ItemList) => {
      retItems = retItems.concat(items)
    }
    await this.handleSearch(params, callback)

    if (!isFilter(event)) {
      const pageSize = Number.parseInt((event.queryStringParameters && event.queryStringParameters.pageSize) || '10')
      const page = Number.parseInt((event.queryStringParameters && event.queryStringParameters.page) || '-1')
      retItems = Pagination.goToPage(retItems, page, pageSize, retItems.length)
    }

    // FIXME: Change return type to an object that includes the total number of
    //  images found, so that client knows how much to paginate
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    return new HttpResponse(200, '', {
      headers: {
        'Content-Type': 'application/json'
      },
      body: retItems
    })
  }

  async dimension(event: APIGatewayProxyEventV2) {
    let dimension = (event.pathParameters && event.pathParameters.dimension) as string
    const isNumeric = event.queryStringParameters && event.queryStringParameters.numeric === 'true'
    const attrExpNames: Record<string, string> = {}

    // Make dimension singular because that's how the
    // DynamoDB table attributes are named
    dimension = singular(dimension)

    attrExpNames['#dimension'] = dimension
    const params: DocumentClient.QueryInput = {
      ExpressionAttributeNames: attrExpNames,
      ProjectionExpression: '#dimension',
      TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string
    }

    this.addFilter((event.queryStringParameters && event.queryStringParameters.filter) as Filter, params)
    this.addEnabledOnlyCondition(params)

    const {
      callback,
      results
    } = this.prepareCallback({
      dimension,
      event,
      isNumeric
    })
    await this.handleSearch(params, callback)
    return new HttpResponse(200, '', {
      body: results
    })
  }

  /**
   * This function returns a JSON object with the fields:
   *   callback: This function creates search "facets". Facets are nothing more than search results grouped by
   *     the unique categories associated with a dimension, along with the count for each group of categories.
   *   results: The results produced by the callback as an array of objects, where each object is of type Dimension
   * @param dimension - The field which unique values (aka categories) are to be grouped and a count of each group produced
   * @param event - The AWS API Gateway event that triggered this Lambda
   * @param isNumeric - Boolean that when true means that ranging such be applied. This applies to dimensions that are numeric in nature,
   *   for example Age. If the dimension is not numeric (E.g. Stain) then this boolean is ignored even if True.
   * @param initialResults - If provided, additional results are added to this array. Useful for merging previous results with
   *   new ones. The counts for each category are added onto the counts in the initial results
   * @private
   */
  private prepareCallback({
    dimension,
    event,
    isNumeric,
    results: initialResults
  }: PrepareCallbackArgs): { callback: (scanOutput: DocumentClient.ScanOutput, items: DocumentClient.ItemList) => void, results: Dimension[] } {
    /*
     * The idiom of passing 'results' below to the constructor of Map of reduce() init arg is used because DynamoDB
     * scan() paginates results, so the callback defined below will get called several times. This way we can pass 'results'
     * results from previous to next call of callback, as many times as necessary until scan results reaches the end.
     */
    const results: Dimension[] = []
    let temp: Dimension[] = initialResults || []
    return {
      callback: (scanOutput: DocumentClient.ScanOutput, items: DocumentClient.ItemList) => {
        /*
         * Ranging only applies to dimensions that are numeric
         * in nature only. Yet we do this here for all for sake of simplicity,
         * namely generating a RangeMap needlessly if the dimension in question
         * is not numeric in nature.
         */
        const interval = Number.parseInt((event.queryStringParameters && event.queryStringParameters.interval) || '10')
        const max = Number.parseInt((event.queryStringParameters && event.queryStringParameters.max) || '90')
        const start = Number.parseInt((event.queryStringParameters && event.queryStringParameters.start) || interval.toString())
        const ranges: RangeMap = new RangeMap(interval, max, start)
        temp = Array.from(items.reduce((prev: Map<string | number, Dimension>, cur: DocumentClient.AttributeMap) => {
          const category = Number.isInteger(cur[dimension]) && isNumeric ? cur[dimension] : paramCase(`${cur[dimension]}`)
          let obj: Dimension | undefined
          if (!(obj = prev.get(category))) {
            // Seeing this category of the dimension for the first time
            obj = {
              count: 0,
              title: cur[dimension],
              category,
              range: undefined,
              rank: -1
            }
            prev.set(category, obj as Dimension)

            // Ranging applies to dimensions numeric in nature only (E.g. Age) and where
            // caller indeed wants to treat those as range-able (numeric=true in query string params)
            if (Number.isInteger(category) && isNumeric) {
              const rangeInfo = ranges.get(category)
              obj.range = rangeInfo?.range
              obj.rank = rangeInfo?.rank as number
            }
          }
          ++obj.count
          return prev
        }, new Map<string | number, Dimension>(temp.map((obj) => [obj.category, obj]))).values())
          .sort((a, b): number => b.rank - a.rank || rank(dimension, a.title) - rank(dimension, b.title))
        results.length = 0
        for (const d of temp) {
          results.push(d)
        }
      },
      results
    }
  }

  /**
   * Augments the passed in DynamoDB query with the string filter found in the
   * request query string, if any, converting it to a DynamoDB filter. Otherwise, it leaves
   * the DynamoDB query untouched.
   * WARNING: Fairly heavy use of RegEx alert.
   */
  private addFilter(filter: Filter, params: DocumentClient.QueryInput) {
    if (!filter) {
      return
    }

    const exprAttrNames: Record<string, string> = {}
    const exprAttrValues: Record<string, string | number> = {}
    let dynamoDbFilter = filter

    /*
     * Deal with the numeric range categories (E.g. age)
     */
    // First deal with the less than and greater than ranges (the bottom and top ones in the chart)
    for (const m of filter.matchAll(/((\w+)\s*=\s*(?:'(\d+)\+'|'\s*<\s*(\d+)'))/g)) {
      const dimension = m[2]
      const dimensionPlaceholder = `#${dimension.replace(/\s+/g, '')}`
      const greaterThanOrEqualTo = m[3]
      const lt = m[4]
      const num = greaterThanOrEqualTo || lt
      const searchStr = m[1]
      const replaceStr = greaterThanOrEqualTo ? `${dimensionPlaceholder} >= :${num}` : `${dimensionPlaceholder} < :${num}`
      exprAttrNames[dimensionPlaceholder] = dimension
      exprAttrValues[`:${num}`] = Number.parseInt(num)
      dynamoDbFilter = dynamoDbFilter.replace(searchStr, replaceStr)
    }

    // Now deal with the ranges in between
    for (const m of filter.matchAll(/(\w+)\s*=\s*'(\d+)\s*-\s*(\d+)'/g)) {
      const dimension = m[1]
      const dimensionPlaceholder = `#${dimension.replace(/\s+/g, '')}`
      const from = m[2]
      const to = m[3]
      exprAttrNames[dimensionPlaceholder] = dimension
      exprAttrValues[`:${from}`] = Number.parseInt(from)
      exprAttrValues[`:${to}`] = Number.parseInt(to)
      dynamoDbFilter = dynamoDbFilter.replace(m[0], `${dimensionPlaceholder} BETWEEN :${from} AND :${to}`)
    }

    // Deal with the text categories (E.g. stain, region, sex, race, diagnosis)
    for (const m of dynamoDbFilter.matchAll(/(\w+)\s*=\s*'([^']+)'|contains\((\w+)\s*,\s*'([^']+)'\)/g)) {
      /*
       * Globally handle dimension replacement upon the first iteration and
       * first iteration only. Applies to cases where a dimension appears multiple times in the
       * query (I.e. there are multiple predicates for the dimension). Why though? This is idempotent so can run many times
       * harmlessly, right?
       */
      const dimension = m[1] || m[3]
      const dimensionPlaceholder = `#${dimension.replace(/\s+/g, '')}`
      if (!exprAttrNames[dimensionPlaceholder]) {
        exprAttrNames[dimensionPlaceholder] = dimension
        dynamoDbFilter = dynamoDbFilter.replace(new RegExp(dimension, 'g'), dimensionPlaceholder)
      }

      const category = m[2] || m[4]
      const categoryPlaceHolder = `:${category.replace(/\W+/g, '')}`

      // Ensure numeric values are stored as JavaScript numeric type, else DynamoDB
      // returns results because it won't coerce to number strings that  are numeric
      // in nature
      exprAttrValues[categoryPlaceHolder] = category.match(/^\d+$/) ? parseInt(category) : category.replace(/__QUOTE__/g, '\'')
      dynamoDbFilter = dynamoDbFilter.replace(`'${category}'`, categoryPlaceHolder)
    }

    params.FilterExpression = dynamoDbFilter
    params.ExpressionAttributeNames = { ...params.ExpressionAttributeNames, ...exprAttrNames }
    params.ExpressionAttributeValues = exprAttrValues
    console.log(`JMQ: params is ${JSON.stringify(params)}`)
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
  }
}

export default new ImageSearch()
