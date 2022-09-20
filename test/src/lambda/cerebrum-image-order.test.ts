import * as lambda from '../../../src/lambda/cerebrum-image-order'
import { APIGatewayProxyEventV2, APIGatewayProxyStructuredResultV2, Context } from 'aws-lambda'
import merge from 'lodash.merge'
import { axiosClient, dynamoDbClient } from '@exsoinn/aws-sdk-wrappers'
import { allFieldsSearchResult, allFieldsSearchResultPaginated } from '../../fixture/cerebrum-image-dynamodb-search-result.fixture'

const mockOrderEventBody: Readonly<Record<string, any>> = {
  fileNames: ['XE13-009_2_HE_1.mrxs', 'XE13-009_2_Sil_1.mrxs', 'XE12-025_1_HE_1.mrxs'],
  email: 'john.smith@acme.com'
}
const jestGlobal = global as any
let event: APIGatewayProxyEventV2
describe('cerebrum-image-order', () => {
  beforeEach(() => {
    jest.clearAllMocks()
    event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
  })

  it('submits image order for fulfillment', async () => {
    const currentTime = new Date('2021-12-27 00:00:00 UTC')
    // @ts-ignore
    const dateSpy = jest.spyOn(global, 'Date').mockImplementation(() => currentTime)
    // @ts-ignore
    axiosClient.post.mockResolvedValueOnce({
      status: 202
    })
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
    expect(axiosClient.post).toHaveBeenCalledWith(`https://${process.env.FULFILLMENT_HOST}/cerebrum-image-orders/${jestGlobal.dummyOrderId}/fulfill`)
    dateSpy.mockRestore()
  })

  it('handles order with filter specified', async () => {
    const currentTime = new Date('2021-12-27 00:00:00 UTC')
    // @ts-ignore
    const dateSpy = jest.spyOn(global, 'Date').mockImplementation(() => currentTime)
    const order: Record<string, any> = {}
    merge(order, mockOrderEventBody)
    order.fileNames = undefined
    event.body = JSON.stringify(order)

    event.queryStringParameters = {
      filter: "age = '84 - 89'"
    }

    // @ts-ignore
    dynamoDbClient.scan.mockResolvedValueOnce(allFieldsSearchResultPaginated)
    // @ts-ignore
    dynamoDbClient.scan.mockResolvedValueOnce(allFieldsSearchResult)

    // @ts-ignore
    axiosClient.post.mockResolvedValueOnce({
      status: 202
    })

    const res = await lambda.create(event, {} as Context, jest.fn()) as APIGatewayProxyStructuredResultV2

    expect(res.statusCode).toEqual(202)
    expect(JSON.parse(res.body as string)).toEqual({
      orderId: jestGlobal.dummyOrderId,
      email: mockOrderEventBody.email,
      fileNames: allFieldsSearchResult.Items!.map(e => e.fileName).concat(allFieldsSearchResult.Items!.map(e => e.fileName)),
      filter: "age = '84 - 89'",
      created: currentTime.toISOString()
    })

    const expectedParams = {
      ExpressionAttributeValues: {
        ':84': 84,
        ':89': 89
      },
      ExpressionAttributeNames: {
        '#age': 'age'
      },
      FilterExpression: '#age BETWEEN :84 AND :89',
      TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string
    }
    expect(dynamoDbClient.scan).toHaveBeenNthCalledWith(1, expectedParams)
    expect(dynamoDbClient.scan).toHaveBeenNthCalledWith(2, {
      ExclusiveStartKey: {
        foo: 'bar'
      },
      ...expectedParams
    })
    dateSpy.mockRestore()
  })

  it('handles unexpected errors', async () => {
    const mockError = 'THIS IS A TEST: Problem creating image Zip'
    // @ts-ignore
    dynamoDbClient.put.mockRejectedValueOnce(mockError)
    event.body = JSON.stringify(mockOrderEventBody)
    const res = await lambda.create(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 500,
      body: JSON.stringify({
        message: `Something went wrong, ${mockError}`
      }, null, ' ')
    })
    expect((axiosClient as any).post).toHaveBeenCalledTimes(0)
  })

  it('returns error when email is missing', async () => {
    const order: Record<string, any> = {}
    merge(order, mockOrderEventBody)
    order.email = ''
    event.body = JSON.stringify(order)
    const res = await lambda.create(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 401,
      body: JSON.stringify({
        message: 'Request is either empty or invalid'
      }, null, ' ')
    })
    expect((axiosClient as any).post).toHaveBeenCalledTimes(0)
  })

  it('returns error when order contains no files and no filter', async () => {
    const order: Record<string, any> = {}
    merge(order, mockOrderEventBody)
    order.fileNames = undefined
    event.body = JSON.stringify(order)
    const res = await lambda.create(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 401,
      body: JSON.stringify({
        message: 'Request is either empty or invalid'
      }, null, ' ')
    })
    expect((axiosClient as any).post).toHaveBeenCalledTimes(0)
  })

  it('returns error when order is empty', async () => {
    event.body = ''
    const res = await lambda.create(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 401,
      body: JSON.stringify({
        message: 'Request is either empty or invalid'
      }, null, ' ')
    })
    expect((axiosClient as any).post).toHaveBeenCalledTimes(0)
  })
})
