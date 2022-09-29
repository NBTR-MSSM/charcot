import { APIGatewayProxyEventV2, APIGatewayProxyHandlerV2 } from 'aws-lambda'
import { dynamoDbClient, HttpResponse, lambdaWrapper, sqsClient } from '@exsoinn/aws-sdk-wrappers'
import { CerebrumImageOrder, Filter } from '../types/charcot.types'
import { v4 as uuidGenerator } from 'uuid'
import imageSearch from '../service/image-search'
import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client'

const parseOrder = async (event: APIGatewayProxyEventV2): Promise<CerebrumImageOrder | undefined> => {
  const order = event.body
  if (!order) {
    return undefined
  }
  const orderObj = JSON.parse(order)
  const filter: Filter = (event.queryStringParameters && event.queryStringParameters.filter) || orderObj.filter
  if (!orderObj.email || (!orderObj.fileNames && !filter)) {
    return undefined
  }
  const orderId = uuidGenerator()
  return {
    orderId,
    email: orderObj.email as string,
    fileNames: orderObj.fileNames || await fetchFileNames(filter),
    filter,
    created: new Date().toISOString()
  }
}

const fetchFileNames = async (filter: Filter): Promise<string[]> => {
  const res = await imageSearch.search(filter)
  const items = res.body as DocumentClient.ItemList
  console.log(`JMQ: items is ${JSON.stringify(items)}`)
  return items.map((e) => e.fileName)
}

export const create: APIGatewayProxyHandlerV2 = lambdaWrapper(async (event: APIGatewayProxyEventV2) => {
  try {
    const order: CerebrumImageOrder | undefined = await parseOrder(event)
    if (order) {
      await dynamoDbClient.put({
        TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME,
        Item: order
      })
      await sqsClient.send(process.env.CEREBRUM_IMAGE_ORDER_QUEUE_URL as string, {
        orderId: order.orderId
      })

      return new HttpResponse(202, 'Your order is being processed, you will get an email soon', {
        body: order
      })
    } else {
      return new HttpResponse(401, 'Request is either empty or invalid')
    }
  } catch (e) {
    return new HttpResponse(500, `Something went wrong, ${e}`)
  }
})
