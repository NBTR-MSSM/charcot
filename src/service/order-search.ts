import { APIGatewayProxyEventV2 } from 'aws-lambda'
import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client'
import Search from './search'
import { dynamoDbClient, HttpResponse } from '@exsoinn/aws-sdk-wrappers'
import userManagement from './user-management'
import { CerebrumImageOrder, OrderRetrievalOutput, OrderTotals } from '../types/charcot.types'

const cancelEligibleStatuses = new Set().add('received').add('processing')

/**
 * Enriches the order transaction passed in with information
 * to the user that created the order.
 */
const populateUserData = async (transaction: DocumentClient.AttributeMap) => {
  const response = await userManagement.retrieve(transaction.email)
  const user = JSON.parse(response.toAwsApiGatewayFormat().body)

  const userAttrs: Record<string, string> = {}
  const firstClassAttributes = ['requester', 'family_name', 'institutionName']
  for (const attr of Object.entries(user)) {
    const name = attr[0]
    const value = attr[1]
    if (firstClassAttributes.includes(name)) {
      transaction[name] = value
    }
    userAttrs[name] = value as string
  }

  transaction.userAttributes = userAttrs
}

const sort = <T extends Record<string, string | unknown>>(items: T[], sortBy: string, sortOrder: 'desc' | 'asc') => {
  const comparator = (a: T, b: T, field = sortBy): number => {
    const left = a[field]
    const right = b[field]

    // Determine if we're sorting numeric or string values. For everything else
    // we don't support sorting - just return 0
    let ret = 0
    if (typeof left === 'number' && typeof right === 'number') {
      ret = sortOrder === 'desc' ? right - left : left - right
    } else if (typeof left === 'string' && typeof right === 'string') {
      let strOne = left.toLowerCase()
      let strTwo = right.toLowerCase()
      if (sortOrder === 'desc') {
        strOne = right.toLowerCase()
        strTwo = left.toLowerCase()
      }
      if (strOne < strTwo) {
        ret = -1
      } else if (strOne > strTwo) {
        ret = 1
      } else {
        ret = 0
      }
    } else {
      ret = 0
    }
    // If we have a tie, use create timestamp to break it
    return ret === 0 ? comparator(a, b, 'created') : ret
  }

  items.sort(comparator)
}

/**
 * Given an array (0-based) of items, a page and a page size (I.e. number of items per page) returns the items in the array
 * in that page.
 * Example:
 *   13 items, pageSize = 5, page = 3
 *   first = (5 * 3) - 5 = 10
 *   last = (5 * 3) - (13 % 5) = 15 - 3 = 12
 *   (1-based) 1 2 3 4 5 6 7 8 9 10 11 12 13
 *   (0-based) 0 1 2 3 4 5 6 7 8 09 10 11 12
 *
 *  5 items, pageSize = 1, page = 2
 *   first = (1 * 2) - 1 = 1
 *   last = (1 * 2) - (5 % 1) = 2 - 0 = 2
 *   (1-based) 1 2 3 4 5 6 7 8 9 10 11 12 13
 *   (0-based) 0 1 2 3 4 5 6 7 8 09 10 11 12
 */
const goToPage = (items: DocumentClient.ItemList, page: number, pageSize: number, orderCount: number) => {
  // If page is not a positive value, or all items fit in a single page,
  // just grab all the records
  if (page < 1 || orderCount <= pageSize) {
    return items
  }
  const first = (pageSize * page) - pageSize
  const last = (pageSize * page)
  return items.slice(first, last)
}

class OrderSearch extends Search {
  /**
   * Retrieves orders and paginates as appropriate
   */
  async retrieve(event: APIGatewayProxyEventV2 | string): Promise<Record<string, unknown>> {
    let retItems: DocumentClient.ItemList = []
    let retBody: OrderRetrievalOutput | Record<string, unknown> = {}
    if (typeof event !== 'string') {
      // Get info across all orders
      const pageSize = Number.parseInt((event.queryStringParameters && event.queryStringParameters.pageSize) || '10')
      const page = Number.parseInt((event.queryStringParameters && event.queryStringParameters.page) || '-1')
      const sortBy = (event.queryStringParameters && event.queryStringParameters.sortBy) || 'created'
      const sortOrder = (event.queryStringParameters && event.queryStringParameters.sortOrder) || 'desc'
      const totals = await this.obtainTotals()
      const { orderCount } = totals
      const totalPages = Math.ceil(orderCount / pageSize)
      if (page > totalPages && totalPages > 0) {
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        return new HttpResponse(401, `Requested page ${page} is out of bounds (only ${totalPages} available at ${pageSize} items per page)`)
      } else if (totalPages === 0) {
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        return new HttpResponse(200, 'No records found', {
          orders: []
        })
      }

      const params: DocumentClient.QueryInput = {
        TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME as string,
        ExpressionAttributeNames: {
          '#orderId': 'orderId',
          '#email': 'email',
          '#created': 'created',
          '#filter': 'filter',
          '#status': 'status',
          '#fulfilled': 'fulfilled',
          '#remark': 'remark',
          '#size': 'size',
          '#fileCount': 'fileCount'
        },
        ProjectionExpression: '#orderId, #email, #created, #filter, #status, #fulfilled, #remark, #size, #fileCount'
      }

      /*
       * Side Note: It seems inefficient to have to load all DynamoDB results into memory in order to sort,
       * but unfortunately DynamoDB scan operation doesn't support sorting.
       */
      const callback = (scanOutput: DocumentClient.ScanOutput, items: DocumentClient.ItemList) => {
        retItems = retItems.concat(items)
        // console.log(`JMQ: retItems is ${JSON.stringify(retItems)}`)
      }

      await this.handleSearch(params, callback)

      // Enrich each order record
      for (const item of retItems) {
        await populateUserData(item)
        item.isCancellable = cancelEligibleStatuses.has(item.status)
      }

      // apply sorting
      if (sortOrder === 'asc' || sortOrder === 'desc') {
        sort(retItems, sortBy, sortOrder)
      }

      retItems = goToPage(retItems, page, pageSize, orderCount)

      retBody = {
        pageSize,
        totalPages,
        page,
        ...totals,
        orders: []
      }
    } else {
      // A specific order (aka request) has been requested
      const res = await dynamoDbClient.get({
        TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME,
        Key: { orderId: event }
      })
      const item = res.Item
      if (!item) {
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        return new HttpResponse(200, `Request ${event} not found`, {
          orders: []
        })
      }
      await populateUserData(item)
      item.isCancellable = cancelEligibleStatuses.has(item.status)
      retItems.push(item)
    }

    retBody.orders = retItems as CerebrumImageOrder[]
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    return new HttpResponse(200, '', retBody
    )
  }

  async obtainTotals(): Promise<OrderTotals> {
    const params: DocumentClient.QueryInput = {
      TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME as string,
      ExpressionAttributeNames: {
        '#size': 'size',
        '#slides': 'filesProcessed',
        '#email': 'email'
      },
      ProjectionExpression: '#size, #slides, #email'
    }
    let size = 0
    let slides = 0
    let orderCount = 0
    const uniqueUsers = new Set()
    const callback = (scanOutput: DocumentClient.ScanOutput, items: DocumentClient.ItemList) => {
      size = items.reduce((accumulator, currentValue) => accumulator + currentValue.size, size)
      slides = items.reduce((accumulator, currentValue) => accumulator + (currentValue.filesProcessed && currentValue.filesProcessed.length), slides)
      orderCount += items.length
      items.forEach(e => {
        uniqueUsers.add(e.email)
      })
    }
    await this.handleSearch(params, callback)
    return {
      size,
      slides,
      orderCount,
      uniqueUsers: uniqueUsers.size
    }
  }
}

export default new OrderSearch()
