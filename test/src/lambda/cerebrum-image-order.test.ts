import * as lambda from '../../../src/lambda/cerebrum-image-order'
import { APIGatewayProxyEventV2, APIGatewayProxyStructuredResultV2, Context } from 'aws-lambda'
import merge from 'lodash.merge'
import { cognitoIdentityServiceProviderClient, dynamoDbClient, sqsClient } from '@exsoinn/aws-sdk-wrappers'
import {
  allFieldsScanResult,
  allFieldsScanResultPaginated
} from '../../fixture/cerebrum-image-image.fixture'
import {
  orderScanResultFactory,
  orderOutputFactory,
  sortedOrderOutput
} from '../../fixture/cerebrum-image-order.fixture'
import { userFactory } from '../../fixture/cerebrum-image-user.fixture'
import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client'

const mockCreateOrderEventBody: Readonly<Record<string, string | string[]>> = {
  fileNames: ['XE13-009_2_HE_1.mrxs', 'XE13-009_2_Sil_1.mrxs', 'XE12-025_1_HE_1.mrxs'],
  email: 'john.smith@acme.com'
}

const jestGlobal = global as unknown as Record<string, string>
let event: APIGatewayProxyEventV2
describe('cerebrum-image-order', () => {
  beforeEach(() => {
    jest.clearAllMocks()
    event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
  })

  it('retrieves all orders', async () => {
    prepareOrderRetrieveMocks()
    const res = await lambda.retrieve(event, {} as Context, jest.fn()) as APIGatewayProxyStructuredResultV2
    expect(JSON.parse(res.body as string)).toEqual(orderOutputFactory())
  })

  it('does not attempt to cancel an order that does not exist', async () => {
    event.pathParameters = {
      orderId: 'mno123',
      requester: 'joquijada2010@gmail.com'
    }

    // @ts-ignore
    dynamoDbClient.get.mockResolvedValueOnce({
      Item: undefined
    })

    const res = await lambda.cancel(event, {} as Context, jest.fn()) as APIGatewayProxyStructuredResultV2
    expect(res).toEqual({
      statusCode: 404,
      body: JSON.stringify({
        message: 'Request mno123 not found'
      }, null, ' ')
    })
  })

  it('rejects cancel request for an order that is not cancellable', async () => {
    event.pathParameters = {
      orderId: 'mno123',
      requester: 'joquijada2010@gmail.com'
    }

    const scanOutput = orderScanResultFactory([5], true) as DocumentClient.GetItemOutput
    scanOutput.Item!.status = 'processed'

    // @ts-ignore
    dynamoDbClient.get.mockResolvedValueOnce(scanOutput)

    // @ts-ignore
    cognitoIdentityServiceProviderClient.adminGetUser.mockImplementationOnce((params: cognitoIdentityServiceProviderClient.AdminGetUserRequest) => {
      const user = userFactory()
      user.Username = params.Username
      return { promise: () => Promise.resolve(user) }
    })

    const res = await lambda.cancel(event, {} as Context, jest.fn()) as APIGatewayProxyStructuredResultV2
    expect(res).toEqual({
      statusCode: 401,
      body: JSON.stringify({
        message: 'Request in status processed cannot be canceled'
      }, null, ' ')
    })
  })

  it('cancels an order', async () => {
    event.pathParameters = {
      orderId: 'mno123',
      requester: 'joquijada2010@gmail.com'
    }
    event.queryStringParameters = {
      requester: 'test@test.com'
    }
    // @ts-ignore
    dynamoDbClient.get.mockResolvedValueOnce(orderScanResultFactory([5], true))

    // @ts-ignore
    cognitoIdentityServiceProviderClient.adminGetUser.mockImplementationOnce((params: cognitoIdentityServiceProviderClient.AdminGetUserRequest) => {
      const user = userFactory()
      user.Username = params.Username
      return { promise: () => Promise.resolve(user) }
    })

    const res = await lambda.cancel(event, {} as Context, jest.fn()) as APIGatewayProxyStructuredResultV2
    expect(res).toEqual({
      statusCode: 200,
      body: JSON.stringify({
        message: 'Operation successful'
      }, null, ' ')
    })

    expect(dynamoDbClient.update).toHaveBeenCalledWith({
      TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME,
      Key: { orderId: 'mno123' },
      UpdateExpression: 'SET #status = :status, #remark = :remark',
      ExpressionAttributeNames: {
        '#status': 'status',
        '#remark': 'remark'
      },
      ExpressionAttributeValues: {
        ':status': 'cancel-requested',
        ':remark': `Cancel requested by test@test.com on ${new Date().toUTCString()}`
      }
    })
  })

  it('defaults to sorting by created timestamp if request asks to sort by non-numeric and non-string field', async () => {
    event.queryStringParameters = {
      sortOrder: 'desc',
      sortBy: 'fileNames'
    }
    prepareOrderRetrieveMocks()
    const res = await lambda.retrieve(event, {} as Context, jest.fn()) as APIGatewayProxyStructuredResultV2
    expect(JSON.parse(res.body as string)).toEqual(sortedOrderOutput([5, 4, 3, 2, 1]))
  })

  it('sorts by requested field in descending order during order retrieval', async () => {
    event.queryStringParameters = {
      sortOrder: 'desc',
      sortBy: 'email'
    }
    prepareOrderRetrieveMocks()
    const res = await lambda.retrieve(event, {} as Context, jest.fn()) as APIGatewayProxyStructuredResultV2
    expect(JSON.parse(res.body as string)).toEqual(sortedOrderOutput([2, 1, 3, 5, 4]))
  })

  it('sorts numeric field in ascending order during order retrieval', async () => {
    event.queryStringParameters = {
      sortOrder: 'asc',
      sortBy: 'created'
    }
    prepareOrderRetrieveMocks()
    const res = await lambda.retrieve(event, {} as Context, jest.fn()) as APIGatewayProxyStructuredResultV2
    expect(JSON.parse(res.body as string)).toEqual(sortedOrderOutput([1, 2, 3, 4, 5]))
  })

  it('correct items for requested page are selected during order retrieval', async () => {
    event.queryStringParameters = {
      pageSize: '1',
      page: '2'
    }
    prepareOrderRetrieveMocks()
    const res = await lambda.retrieve(event, {} as Context, jest.fn()) as APIGatewayProxyStructuredResultV2
    const expected = orderOutputFactory([4])
    expected.page = 2
    expected.pageSize = 1
    expected.totalPages = 5
    expect(JSON.parse(res.body as string)).toEqual(expected)
  })

  it('retrieves correct list of items in last page when total items is not a multiple of page size', async () => {
    // Try edge case where total number of items doesn't divide evenly among
    // all pages (I.e. totalItems % pageSize > 0), in other words total number of items is not
    // a multiple of page size
    event.queryStringParameters = {
      pageSize: '2',
      page: '3'
    }
    prepareOrderRetrieveMocks()
    const res = await lambda.retrieve(event, {} as Context, jest.fn()) as APIGatewayProxyStructuredResultV2
    const expected = orderOutputFactory([1])
    expected.page = 3
    expected.pageSize = 2
    expected.totalPages = 3
    expect(JSON.parse(res.body as string)).toEqual(expected)
  })

  it('returns non-success during order retrieval if page request is out of bounds', async () => {
    event.queryStringParameters = {
      pageSize: '1',
      page: '6'
    }
    prepareOrderRetrieveMocks({
      mockScanOutputTwo: 'skip'
    })
    const res = await lambda.retrieve(event, {} as Context, jest.fn()) as APIGatewayProxyStructuredResultV2
    expect(res).toEqual({
      statusCode: 401,
      body: JSON.stringify({
        message: 'Requested page 6 is out of bounds (only 5 available at 1 items per page)'
      }, null, ' ')
    })
  })

  it('returns empty results during order retrieval if no orders found', async () => {
    prepareOrderRetrieveMocks({
      mockScanOutputOne: {
        Items: []
      },
      mockScanOutputTwo: 'skip'
    })
    const res = await lambda.retrieve(event, {} as Context, jest.fn()) as APIGatewayProxyStructuredResultV2
    expect(res).toEqual({
      statusCode: 200,
      body: JSON.stringify({
        message: 'No records found',
        orders: []
      }, null, ' ')
    })
  })

  it('submits image order for fulfillment', async () => {
    const currentTime = new Date('2021-12-27 00:00:00 UTC')
    // @ts-ignore
    const dateSpy = jest.spyOn(global, 'Date').mockImplementation(() => currentTime)
    event.body = JSON.stringify(mockCreateOrderEventBody)
    const res = await lambda.create(event, {} as Context, jest.fn()) as APIGatewayProxyStructuredResultV2
    expect(res.statusCode).toEqual(202)
    expect(JSON.parse(res.body as string)).toEqual({
      orderId: jestGlobal.dummyOrderId,
      recordNumber: 0,
      ...mockCreateOrderEventBody,
      created: currentTime.getTime(),
      remark: 'Your request has been received by Mount Sinai Charcot',
      status: 'received'
    })

    expect(dynamoDbClient.put).toHaveBeenLastCalledWith({
      TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME,
      Item: {
        orderId: jestGlobal.dummyOrderId,
        recordNumber: 0,
        created: new Date().getTime(),
        remark: 'Your request has been received by Mount Sinai Charcot',
        status: 'received',
        ...mockCreateOrderEventBody
      }
    })

    expect(sqsClient.send).toHaveBeenCalledWith(process.env.CEREBRUM_IMAGE_ORDER_QUEUE_URL, {
      orderId: jestGlobal.dummyOrderId
    })
    dateSpy.mockRestore()
  })

  it('handles order with filter specified', async () => {
    const currentTime = new Date('2021-12-27 00:00:00 UTC')
    // @ts-ignore
    const dateSpy = jest.spyOn(global, 'Date').mockImplementation(() => currentTime)
    const order: Record<string, string | undefined> = {}
    merge(order, mockCreateOrderEventBody)
    order.fileNames = undefined
    event.body = JSON.stringify(order)

    event.queryStringParameters = {
      filter: 'age = \'84 - 89\''
    }

    // @ts-ignore
    dynamoDbClient.scan.mockResolvedValueOnce(allFieldsScanResultPaginated)
    // @ts-ignore
    dynamoDbClient.scan.mockResolvedValueOnce(allFieldsScanResult)

    const res = await lambda.create(event, {} as Context, jest.fn()) as APIGatewayProxyStructuredResultV2

    expect(res.statusCode).toEqual(202)
    expect(JSON.parse(res.body as string)).toEqual({
      orderId: jestGlobal.dummyOrderId,
      recordNumber: 0,
      email: mockCreateOrderEventBody.email,
      fileNames: allFieldsScanResult.Items!.map(e => e.fileName).concat(allFieldsScanResult.Items!.map(e => e.fileName)),
      filter: 'age = \'84 - 89\'',
      created: currentTime.getTime(),
      remark: 'Your request has been received by Mount Sinai Charcot',
      status: 'received'
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
    event.body = JSON.stringify(mockCreateOrderEventBody)
    const res = await lambda.create(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 500,
      body: JSON.stringify({
        message: `Something went wrong, ${mockError}`
      }, null, ' ')
    })
    expect(sqsClient.send).toHaveBeenCalledTimes(0)
  })

  it('returns error when email is missing', async () => {
    const order: Record<string, string> = {}
    merge(order, mockCreateOrderEventBody)
    order.email = ''
    event.body = JSON.stringify(order)
    const res = await lambda.create(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 401,
      body: JSON.stringify({
        message: 'Request is either empty or invalid'
      }, null, ' ')
    })
    expect(sqsClient.send).toHaveBeenCalledTimes(0)
  })

  it('returns error when order contains no files and no filter', async () => {
    const order: Record<string, string | undefined> = {}
    merge(order, mockCreateOrderEventBody)
    order.fileNames = undefined
    event.body = JSON.stringify(order)
    const res = await lambda.create(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 401,
      body: JSON.stringify({
        message: 'Request is either empty or invalid'
      }, null, ' ')
    })
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
    expect(sqsClient.send).toHaveBeenCalledTimes(0)
  })
})

function prepareOrderRetrieveMocks({
  mockScanOutputOne = undefined,
  mockScanOutputTwo = undefined
}: Record<string, DocumentClient.ScanOutput | undefined | 'skip'> = {}) {
  const orderScanResult = orderScanResultFactory() as DocumentClient.ScanOutput
  if (mockScanOutputOne !== 'skip') {
    // @ts-ignore
    dynamoDbClient.scan.mockResolvedValueOnce(mockScanOutputOne || orderScanResult)
  }

  if (mockScanOutputTwo !== 'skip') {
    // @ts-ignore
    dynamoDbClient.scan.mockResolvedValueOnce(mockScanOutputTwo || orderScanResult)

    for (let i = 0; i < orderScanResult.Items!.length; i++) {
      // @ts-ignore
      cognitoIdentityServiceProviderClient.adminGetUser.mockImplementationOnce((params: cognitoIdentityServiceProviderClient.AdminGetUserRequest) => {
        const user = userFactory()
        user.Username = params.Username
        return { promise: () => Promise.resolve(user) }
      })
    }
  }
}
