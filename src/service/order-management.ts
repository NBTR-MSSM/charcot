import { APIGatewayProxyEventV2 } from 'aws-lambda'
import { CerebrumImageOrder, Filter } from '../types/charcot.types'
import { dynamoDbClient, HttpResponse, sqsClient } from '@exsoinn/aws-sdk-wrappers'
import { v4 as uuidGenerator } from 'uuid'
import imageSearch from './image-search'
import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client'
import orderSearch from './order-search'

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
    created: new Date().getTime(),
    status: 'received',
    remark: 'Your request has been received by Mount Sinai Charcot'
  }
}

const fetchFileNames = async (filter: Filter): Promise<string[]> => {
  const res = await imageSearch.search(filter)
  const items = res.body as DocumentClient.ItemList
  // console.log(`JMQ: items is ${JSON.stringify(items)}`)
  return items.map((e) => e.fileName)
}

class OrderManagement {
  async create(event: APIGatewayProxyEventV2) {
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
  }

  /**
   * TODO: bark if I get a cancel request for an order not in a cancellable status.
   */
  async cancel(event: APIGatewayProxyEventV2) {
    const orderId = (event.pathParameters && event.pathParameters.orderId) as string
    const res = await orderSearch.retrieve(orderId)
    const orders = res.orders as DocumentClient.ItemList
    if (orders && orders.length > 0) {
      const order = orders[0]
      if (!order.isCancellable) {
        return new HttpResponse(401, `Request in status ${order.status} cannot be cancelled`)
      }
      const requester = (event.queryStringParameters && event.queryStringParameters.requester) as string
      console.log(`JMQ: request ${orderId} for cancel`)
      await dynamoDbClient.update({
        TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME,
        Key: { orderId },
        UpdateExpression: 'SET #status = :status, #remark = :remark',
        ExpressionAttributeNames: {
          '#status': 'status',
          '#remark': 'remark'
        },
        ExpressionAttributeValues: {
          ':status': 'cancel-requested',
          ':remark': `Cancel requested by ${requester}`
        }
      })
    } else {
      return new HttpResponse(404, `Request ${orderId} not found`)
    }
  }
}

export default new OrderManagement()
