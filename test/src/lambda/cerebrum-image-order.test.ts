import * as lambda from '../../../src/lambda/cerebrum-image-order'
import { APIGatewayProxyEventV2, APIGatewayProxyStructuredResultV2, Context } from 'aws-lambda'
import merge from 'lodash.merge'
import { dynamoDbClient, lambdaClient } from '@exsoinn/aws-sdk-wrappers'

const imageFulfillmentLambdaName = process.env.HANDLE_CEREBRUM_IMAGE_FULFILLMENT_FUNCTION_NAME as string

const mockOrderEventBody: Readonly<Record<string, any>> = {
  fileNames: ['XE13-009_2_HE_1.mrxs', 'XE13-009_2_Sil_1.mrxs', 'XE12-025_1_HE_1.mrxs'],
  email: 'john.smith@acme.com'
}
const jestGlobal = global as any
describe('cerebrum-image-order', () => {
  beforeEach(() => {
    jest.clearAllMocks()
  })
  it('submits image order for fulfillment', async () => {
    const currentTime = new Date('2021-12-27 00:00:00 UTC')
    // @ts-ignore
    const dateSpy = jest.spyOn(global, 'Date').mockImplementation(() => currentTime)
    // @ts-ignore
    lambdaClient.invokeLambda.mockResolvedValueOnce({
      StatusCode: 202
    })
    const event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.body = JSON.stringify(mockOrderEventBody)
    const res = await lambda.create(event, {} as Context, jest.fn()) as APIGatewayProxyStructuredResultV2
    expect(res.statusCode).toEqual(202)
    expect(JSON.parse(res.body as string)).toEqual({
      orderId: jestGlobal.dummyOrderId,
      ...mockOrderEventBody,
      created: currentTime.toISOString()
    })

    expect(dynamoDbClient.put).toHaveBeenLastCalledWith({
      TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME,
      Item: {
        orderId: jestGlobal.dummyOrderId,
        created: new Date().toISOString(),
        ...mockOrderEventBody
      }
    })

    // @ts-ignore
    expect(lambdaClient.invokeLambda.mock.calls[0][0]).toEqual(imageFulfillmentLambdaName)
    // @ts-ignore
    expect(lambdaClient.invokeLambda.mock.calls[0][1]).toEqual('Event')
    // @ts-ignore
    expect(lambdaClient.invokeLambda.mock.calls[0][2]).toEqual(JSON.stringify({ orderId: jestGlobal.dummyOrderId }))
    // @ts-ignore
    expect(lambdaClient.invokeLambda.mock.calls[0][3]).toEqual(imageFulfillmentLambdaName)
    dateSpy.mockRestore()
  })

  it('handles unexpected errors', async () => {
    const mockError = 'THIS IS A TEST: Problem creating image Zip'
    // @ts-ignore
    dynamoDbClient.put.mockRejectedValueOnce(mockError)
    const event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.body = JSON.stringify(mockOrderEventBody)
    const res = await lambda.create(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 500,
      body: JSON.stringify({
        message: `Something went wrong, ${mockError}`
      }, null, ' ')
    })
    expect((lambdaClient as any).invokeLambda).toHaveBeenCalledTimes(0)
  })

  it('returns error when email is missing', async () => {
    const order: Record<string, any> = {}
    merge(order, mockOrderEventBody)
    order.email = ''
    const event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.body = JSON.stringify(order)
    const res = await lambda.create(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 401,
      body: JSON.stringify({
        message: 'Request is either empty or invalid'
      }, null, ' ')
    })
    expect((lambdaClient as any).invokeLambda).toHaveBeenCalledTimes(0)
  })

  it('returns error when order contains no files', async () => {
    const order: Record<string, any> = {}
    merge(order, mockOrderEventBody)
    order.fileNames = undefined
    const event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.body = JSON.stringify(order)
    const res = await lambda.create(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 401,
      body: JSON.stringify({
        message: 'Request is either empty or invalid'
      }, null, ' ')
    })
    expect((lambdaClient as any).invokeLambda).toHaveBeenCalledTimes(0)
  })

  it('returns error when order is empty', async () => {
    const event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.body = ''
    const res = await lambda.create(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 401,
      body: JSON.stringify({
        message: 'Request is either empty or invalid'
      }, null, ' ')
    })
    expect((lambdaClient as any).invokeLambda).toHaveBeenCalledTimes(0)
  })
})
