import { APIGatewayProxyEventV2, APIGatewayProxyHandlerV2 } from 'aws-lambda'
import { axiosClient, dynamoDbClient, HttpResponse, lambdaWrapper } from '@exsoinn/aws-sdk-wrappers'
import { CerebrumImageOrder, Filter } from '../types/charcot.types'
import { v4 as uuidGenerator } from 'uuid'
import imageSearch from '../service/image-search'
import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client'

// TODO: Update unit test to reflect use of filter
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
  // TODO: After some back and forth between user and this Lambda,
  //   the final list of files requested is ready to be sent to processor.
  try {
    const order: CerebrumImageOrder | undefined = await parseOrder(event)
    if (order) {
      await dynamoDbClient.put({
        TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME,
        Item: order
      })
      const resp = await axiosClient.post(`https://${process.env.FULFILLMENT_HOST}/cerebrum-image-orders/${order.orderId}/fulfill`)
      return new HttpResponse(resp.status, 'Your order is being processed, you will get an email soon', {
        body: order
      })
    } else {
      return new HttpResponse(401, 'Request is either empty or invalid')
    }
  } catch (e) {
    return new HttpResponse(500, `Something went wrong, ${e}`)
  }
})
