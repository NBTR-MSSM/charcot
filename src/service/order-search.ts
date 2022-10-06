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
    console.log(`JMQ: OrderSearch.retrieve ${JSON.stringify(event)}`)
    // const attrExpNames: Record<string, string> = {}
    const params: DocumentClient.QueryInput = {
      // ExpressionAttributeNames: attrExpNames,
      TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME as string
    }

    let responseCode = 404
    let retItems: DocumentClient.ItemList = []
    const callback = (items: DocumentClient.ItemList) => {
      retItems = retItems.concat(items)
      responseCode = 200
    }
    await this.handleSearch(params, callback)
    for (const item of retItems) {
      await populateUserData(item)
    }
    return new HttpResponse(responseCode, '', {
      body: retItems
    })
  }
}

export default new OrderSearch()
