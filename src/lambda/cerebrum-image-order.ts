import { APIGatewayProxyEventV2, APIGatewayProxyHandlerV2 } from 'aws-lambda'
import { dynamoDbClient, HttpResponse, lambdaClient, lambdaWrapper } from '@exsoinn/aws-sdk-wrappers'
import { CerebrumImageOrder } from '../types/charcot.types'
import { v4 as uuidGenerator } from 'uuid'
import imageSearch from '../service/image-search'
import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client'

// TODO: Update unit test to reflect use of filter
const parseOrder = async (event: APIGatewayProxyEventV2): Promise<CerebrumImageOrder | undefined> => {
  const order = event.body
  console.log(`JMQ: event is ${JSON.stringify(event)}`)
  if (!order) {
    return undefined
  }
  const orderObj = JSON.parse(order)
  const filter = (event.queryStringParameters && event.queryStringParameters.filter) || orderObj.filter
  console.log(`JMQ: orderObj is ${JSON.stringify(orderObj)}`)
  if (!orderObj.email || (!orderObj.fileNames && !filter)) {
    return undefined
  }
  const orderId = uuidGenerator()
  return {
    orderId,
    email: orderObj.email as string,
    fileNames: orderObj.fileNames || await fetchFileNames(event),
    filter,
    created: new Date().toISOString()
  }
}

const fetchFileNames = async (event: APIGatewayProxyEventV2): Promise<string[]> => {
  const res = await imageSearch.search(event)
  const items = res.body as DocumentClient.ItemList
  return items.map((e) => e.fileName)
}

export const create: APIGatewayProxyHandlerV2 = lambdaWrapper(async (event: APIGatewayProxyEventV2) => {
  // TODO: After some back and forth between user and this Lambda,
  //   the final list of files requested is ready to be sent to processor.
  try {
    const lambdaName = process.env.HANDLE_CEREBRUM_IMAGE_FULFILLMENT_FUNCTION_NAME
    const order: CerebrumImageOrder | undefined = await parseOrder(event)
    if (order) {
      await dynamoDbClient.put({
        TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME,
        Item: order
      })
      const lambdaRes = await (lambdaClient as any).invokeLambda(lambdaName, 'Event', JSON.stringify({ orderId: order.orderId }), lambdaName)
      return new HttpResponse(lambdaRes.StatusCode as number, 'Your order is being processed, you will get an email soon', {
        body: order
      })
    } else {
      return new HttpResponse(401, 'Request is either empty or invalid')
    }
  } catch (e) {
    return new HttpResponse(500, `Something went wrong, ${e}`)
  }
})
