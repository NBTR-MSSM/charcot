import { APIGatewayProxyEventV2 } from 'aws-lambda'
import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client'
import Search from './search'
import { HttpResponse } from '@exsoinn/aws-sdk-wrappers'
import userManagement from './user-management'

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

const sort = (items: DocumentClient.ItemList, sortBy: string, sortOrder: string) => {
  items.sort((a, b) => {
    // Determine if we're sorting numeric or string values
    if (sortBy === 'created') {
      return sortOrder === 'desc' ? b[sortBy] - a[sortBy] : a[sortBy] - b[sortBy]
    } else {
      let strOne = a[sortBy]
      let strTwo = b[sortBy]
      if (sortOrder === 'desc') {
        strOne = b[sortBy]
        strTwo = a[sortBy]
      }
      if (strOne < strTwo) {
        return -1
      } else if (strOne > strTwo) {
        return 1
      } else {
        return 0
      }
    }
  })
}

/**
 * Given a page and page size (I.e. number of items per page) returns the items in the array
 * in that page.
 */
const goToPage = (items: DocumentClient.ItemList, page: number, pageSize: number) => {
  // Calculate the indexes of the first and last items of the page requested
  let last = pageSize * page - 1
  const first = last - pageSize + 1

  // adjust last index if requested page is the last one, and it's not full
  last = last > items.length - 1 ? items.length - 1 : last

  return items.slice(first, last + 1)
}

class OrderSearch extends Search {
  async retrieve(event: APIGatewayProxyEventV2): Promise<Record<string, any>> {
    const pageSize = Number.parseInt((event.queryStringParameters && event.queryStringParameters.pageSize) || '10')
    const page = Number.parseInt((event.queryStringParameters && event.queryStringParameters.page) || '1')
    const sortBy = (event.queryStringParameters && event.queryStringParameters.sortBy) || 'created'
    const sortOrder = (event.queryStringParameters && event.queryStringParameters.sortOrder) || 'desc'
    const orderCount = await this.obtainOrderCount(event)
    const totalPages = Math.ceil(orderCount / pageSize)

    if (page > totalPages && totalPages > 0) {
      return new HttpResponse(404, `Page ${page} is out of bounds (only ${totalPages} available at ${pageSize} items per page)`)
    } else if (totalPages === 0) {
      return new HttpResponse(200, 'No records found', {
        orders: []
      })
    }

    // console.log(`JMQ: pageSize is ${pageSize}, orderCount is ${orderCount}, totalPages is ${totalPages}, page is ${page}, sortBy is ${sortBy}, sortOrder is ${sortOrder}`)

    const params: DocumentClient.QueryInput = {
      TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME as string
    }

    /*
     * Side Note: It seems inefficient to have to load all DynamoDB results in memory and sort,
     * but unfortunately DynamoDB scan operation doesn't support sorting.
     */
    let retItems: DocumentClient.ItemList = []
    const callback = (scanOutput: DocumentClient.ScanOutput, items: DocumentClient.ItemList) => {
      retItems = retItems.concat(items)
      // console.log(`JMQ: retItems is ${JSON.stringify(retItems)}`)
    }

    await this.handleSearch(params, callback)

    for (const item of retItems) {
      await populateUserData(item)
    }

    // apply sorting
    sort(retItems, sortBy, sortOrder)

    retItems = goToPage(retItems, page, pageSize)

    return new HttpResponse(200, '', {
      orderCount,
      pageSize,
      totalPages,
      orders: retItems
    })
  }

  async obtainOrderCount(event: APIGatewayProxyEventV2): Promise<number> {
    const params: DocumentClient.QueryInput = {
      TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME as string,
      Select: 'COUNT'
    }
    let count = 0
    const callback = (scanOutput: DocumentClient.ScanOutput) => {
      if (scanOutput.Count !== undefined) {
        count += scanOutput.Count
      }
    }
    await this.handleSearch(params, callback)
    return count
  }
}

export default new OrderSearch()
