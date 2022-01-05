import { APIGatewayProxyEventV2, APIGatewayProxyHandlerV2 } from 'aws-lambda'
import { dynamoDbClient, HttpResponse, lambdaClient, lambdaWrapper } from '@exsoinn/aws-sdk-wrappers'
import { CerebrumImageOrder } from '../types/charcot.types'
import { v4 as uuidGenerator } from 'uuid'

const parseOrder = (order: string | undefined): CerebrumImageOrder | undefined => {
  if (!order) {
    return undefined
  }
  const orderObj = JSON.parse(order)
  if (!orderObj.email || !orderObj.fileNames) {
    return undefined
  }
  const orderId = uuidGenerator()
  return {
    orderId,
    email: orderObj.email as string,
    fileNames: orderObj.fileNames,
    created: new Date().toISOString()
  }
}
export const create: APIGatewayProxyHandlerV2 = lambdaWrapper(async (event: APIGatewayProxyEventV2) => {
  // TODO: After some back and forth between user and this Lambda,
  //   the final list of files requested is ready to be sent to processor.
  try {
    const lambdaName = process.env.HANDLE_CEREBRUM_IMAGE_FULFILLMENT_FUNCTION_NAME
    const order: CerebrumImageOrder | undefined = parseOrder(event.body)
    if (order) {
      await dynamoDbClient.put({
        TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME,
        Item: order
      })
      const lambdaRes = await lambdaClient.invokeLambda(lambdaName, 'Event', order.orderId, lambdaName)
      return new HttpResponse(lambdaRes.StatusCode as number, 'Your order is being processed, you will get an email soon')
    } else {
      return new HttpResponse(401, 'Request is either empty or invalid')
    }
  } catch (e) {
    return new HttpResponse(500, `Something went wrong, ${e}`)
  }
})
