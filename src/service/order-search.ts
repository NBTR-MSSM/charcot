import { APIGatewayProxyEventV2 } from 'aws-lambda'
import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client'
import Search from './search'
import { HttpResponse, cognitoIdentityServiceProvider } from '@exsoinn/aws-sdk-wrappers'
import { capitalCase } from 'change-case'

const populateUserData = async (transaction: DocumentClient.AttributeMap) => {
  const userData = await cognitoIdentityServiceProvider.adminGetUser({
    UserPoolId: process.env.CEREBRUM_COGNITO_USER_POOL_ID as string,
    Username: transaction.email
  }).promise()
  const userAttrs: Record<string, string> = {}
  for (const attr of userData.UserAttributes || []) {
    const name = attr.Name
    if (!name.startsWith('custom:')) {
      continue
    }
    userAttrs[capitalCase(name.replace('custom:', ''))] = attr.Value as string
  }
  transaction.userAttributes = userAttrs
}

class OrderSearch extends Search {
  async retrieve(event: APIGatewayProxyEventV2): Promise<Record<string, any>> {
    // console.log(`JMQ: OrderSearch.retrieve ${JSON.stringify(event)}`)
    const pageSize = Number.parseInt((event.queryStringParameters && event.queryStringParameters.pageSize) || '10')
    const page = Number.parseInt((event.queryStringParameters && event.queryStringParameters.page) || '1')
    const orderCount = await this.obtainOrderCount(event)
    const totalPages = Math.ceil(orderCount / pageSize)
    if (page > totalPages) {
      return new HttpResponse(404, `Page ${page} is out of bounds (only ${totalPages} available at ${pageSize} items per page)`)
    }

    // console.log(`JMQ: pageSize is ${pageSize}, orderCount is ${orderCount}, totalPages is ${totalPages}, page is ${page}`)

    const params: DocumentClient.QueryInput = {
      TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME as string,
      Limit: pageSize
    }

    let responseCode = 404
    let retItems: DocumentClient.ItemList = []
    let lastEvaluatedKey
    const callback = (scanOutput: DocumentClient.ScanOutput, items: DocumentClient.ItemList) => {
      lastEvaluatedKey = scanOutput.LastEvaluatedKey
      retItems = items
      responseCode = 200
    }

    /*
     * Paginate until we reach our target page. Pass the last evaluated key
     * to pickup where previous page left off. The callback above
     * will capture the results of the last page evaluated only.
     */
    for (let i = 1; i <= page; i++) {
      // console.log(`JMQ: advancing to page ${i}`)
      params.ExclusiveStartKey = lastEvaluatedKey
      await this.handleSearch(params, callback)
    }

    for (const item of retItems) {
      await populateUserData(item)
    }
    return new HttpResponse(responseCode, '', {
      lastEvaluatedKey,
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
