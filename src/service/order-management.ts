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
    recordNumber: 0,
    email: orderObj.email as string,
    intendedUse: orderObj.intendedUse as string,
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
        return new HttpResponse(400, 'Request is either empty or invalid')
      }
    } catch (e) {
      return new HttpResponse(500, `Something went wrong, ${e}`)
    }
  }

  async requestMoreInfo(event: APIGatewayProxyEventV2) {
    const orderId = (event.pathParameters && event.pathParameters.orderId) as string
    const res = await orderSearch.retrieve(orderId)
    const orders = res.orders as DocumentClient.ItemList
    if (orders && orders.length > 0) {
      const order = orders[0]
      if (!order.isMoreInfoRequestable) {
        return new HttpResponse(400, `Request in status ${order.status} not eligible for more info request`)
      }

      const requester = (event.queryStringParameters && event.queryStringParameters.requester) as string
      const infoNeeded = (event.body && JSON.parse(event.body).infoNeeded) as string
      await dynamoDbClient.update({
        TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME,
        Key: {
          orderId,
          recordNumber: 0
        },
        UpdateExpression: 'SET #status = :status, #remark = :remark, #infoNeeded = :infoNeeded',
        ExpressionAttributeNames: {
          '#status': 'status',
          '#remark': 'remark',
          '#infoNeeded': 'infoNeeded'
        },
        ExpressionAttributeValues: {
          ':status': 'more-info-requested',
          ':remark': `[${new Date().toUTCString()}] More info requested by ${requester}`,
          ':infoNeeded': infoNeeded
        }
      })
      await sqsClient.send(process.env.CEREBRUM_IMAGE_ORDER_QUEUE_URL as string, {
        orderId: order.orderId
      })
    } else {
      return new HttpResponse(404, `Request ${orderId} not found`)
    }
  }

  async approve(event: APIGatewayProxyEventV2) {
    const orderId = (event.pathParameters && event.pathParameters.orderId) as string
    const res = await orderSearch.retrieve(orderId)
    const orders = res.orders as DocumentClient.ItemList
    if (orders && orders.length > 0) {
      const order = orders[0]
      if (!order.isApprovable) {
        return new HttpResponse(400, `Request in status ${order.status} cannot be approved`)
      }

      const requester = (event.queryStringParameters && event.queryStringParameters.requester) as string
      await dynamoDbClient.update({
        TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME,
        Key: {
          orderId,
          recordNumber: 0
        },
        UpdateExpression: 'SET #status = :status, #remark = :remark',
        ExpressionAttributeNames: {
          '#status': 'status',
          '#remark': 'remark'
        },
        ExpressionAttributeValues: {
          ':status': 'approved',
          ':remark': `[${new Date().toUTCString()}] Approved by ${requester}`
        }
      })
      await sqsClient.send(process.env.CEREBRUM_IMAGE_ORDER_QUEUE_URL as string, {
        orderId: order.orderId
      })
    } else {
      return new HttpResponse(404, `Request ${orderId} not found`)
    }
  }

  async cancel(event: APIGatewayProxyEventV2) {
    const orderId = (event.pathParameters && event.pathParameters.orderId) as string
    const res = await orderSearch.retrieve(orderId)
    const orders = res.orders as DocumentClient.ItemList
    if (orders && orders.length > 0) {
      const order = orders[0]
      if (!order.isCancellable) {
        return new HttpResponse(400, `Request in status ${order.status} cannot be canceled`)
      }

      const requester = (event.queryStringParameters && event.queryStringParameters.requester) as string
      await dynamoDbClient.update({
        TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME,
        Key: {
          orderId,
          recordNumber: 0
        },
        UpdateExpression: 'SET #status = :status, #remark = :remark',
        ExpressionAttributeNames: {
          '#status': 'status',
          '#remark': 'remark'
        },
        ExpressionAttributeValues: {
          ':status': 'cancel-requested',
          ':remark': `[${new Date().toUTCString()}] Cancel requested by ${requester}`
        }
      })
    } else {
      return new HttpResponse(404, `Request ${orderId} not found`)
    }
  }
}

export default new OrderManagement()
