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

const sort = <T extends Record<string, any>>(items: T[], sortBy: string, sortOrder: 'desc' | 'asc') => {
  const comparator = (a: T, b: T, field = sortBy): number => {
    const left = a[field]
    const right = b[field]

    // Determine if we're sorting numeric or string values. For everything else
    // we don't support sorting - just return 0
    let ret = 0
    if (typeof left === 'number' && typeof right === 'number') {
      ret = sortOrder === 'desc' ? right - left : left - right
    } else if (typeof left === 'string' && typeof right === 'string') {
      let strOne = left
      let strTwo = right
      if (sortOrder === 'desc') {
        strOne = right
        strTwo = left
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
    // If we have a tie, use request created timestamp to break it
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
const goToPage = (items: DocumentClient.ItemList, page: number, pageSize: number) => {
  const first = (pageSize * page) - pageSize
  let last = (pageSize * page)
  /*
   * Is the page requested the last one? If so adjust the 'last' item index if last
   * page is not full, I.e. num of items in last page < pageSize
   */
  const numItemsOfLastPage = items.length % pageSize
  if (page >= items.length / pageSize && numItemsOfLastPage) {
    last = last - numItemsOfLastPage + 1
  }
  return items.slice(first, last)
}

class OrderSearch extends Search {
  /**
   * Retrieves orders and paginates as appropriate
   */
  async retrieve(event: APIGatewayProxyEventV2 | string): Promise<Record<string, any>> {
    let retItems: DocumentClient.ItemList = []
    let retBody: OrderRetrievalOutput | Record<string, any> = {}
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
        return new HttpResponse(401, `Page ${page} is out of bounds (only ${totalPages} available at ${pageSize} items per page)`)
      } else if (totalPages === 0) {
        return new HttpResponse(200, 'No records found', {
          orders: []
        })
      }

      const params: DocumentClient.QueryInput = {
        TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME as string
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

      // apply sorting
      if (sortOrder === 'asc' || sortOrder === 'desc') {
        sort(retItems, sortBy, sortOrder)
      }

      // If page is 0 or a negative value, grab all the records
      if (page > 0) {
        retItems = goToPage(retItems, page, pageSize)
      }

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
      if (res.Item) {
        retItems.push(res.Item)
      }
    }

    // Enrich each order record
    for (const item of retItems) {
      await populateUserData(item)
      item.isCancellable = cancelEligibleStatuses.has(item.status)
    }

    retBody.orders = retItems as CerebrumImageOrder[]
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
