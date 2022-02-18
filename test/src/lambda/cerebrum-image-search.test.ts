import * as lambda from '../../../src/lambda/cerebrum-image-search'
import { APIGatewayProxyEventV2, Context } from 'aws-lambda'
import merge from 'lodash.merge'
import { dynamoDbClient } from '@exsoinn/aws-sdk-wrappers'

const jestGlobal = global as any
describe('cerebrum-image-search', () => {
  beforeEach(() => {
    jest.clearAllMocks()
  })
  it('handles image search', async () => {
    const files = ['file1', 'file2', 'file3']
    const items = files.map(f => (
      {
        fileName: f,
        region: 'foo',
        sex: 'Male',
        race: 'Black',
        stain: 'XYZ',
        uploadDate: '12/20/2022'
      }))
    // @ts-ignore
    dynamoDbClient.query.mockResolvedValueOnce({
      ConsumedCapacity: {},
      Count: 3,
      Items: items
    })
    const event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.queryStringParameters = {
      sex: 'Male',
      race: 'Black',
      stain: 'XYZ'
    }
    const res = await lambda.handle(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 200,
      body: JSON.stringify(items, null, ' ')
    })
    expect(dynamoDbClient.query).toHaveBeenCalledWith({
      ExpressionAttributeValues: {
        ':sex': 'Male',
        ':race': 'Black',
        ':stain': 'XYZ'
      },
      ExpressionAttributeNames: {
        '#race': 'race',
        '#sex': 'sex',
        '#stain': 'stain'
      },
      TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string,
      IndexName: 'sexIndex',
      KeyConditionExpression: '#sex = :sex',
      FilterExpression: '#stain = :stain AND #race = :race'
    })
  })

  it('handles unexpected errors', async () => {
    const mockError = 'THIS IS A TEST: Problem creating image Zip'
    // @ts-ignore
    dynamoDbClient.query.mockRejectedValueOnce(mockError)
    const event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.queryStringParameters = {
      sex: 'Male',
      race: 'Black',
      stain: 'XYZ',
      email: 'john.smith@acme.com'
    }
    const res = await lambda.handle(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 500,
      body: JSON.stringify({
        message: `Something went wrong, ${mockError}`
      }, null, ' ')
    })
  })

  it('returns error no search criteria has been specified', async () => {
    const mockError = 'THIS IS A TEST: Problem creating image Zip'
    // @ts-ignore
    dynamoDbClient.query.mockRejectedValueOnce(mockError)
    const event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.queryStringParameters = {
      foo: 'bar'
    }
    const res = await lambda.handle(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 401,
      body: JSON.stringify({
        message: `Request is invalid: ${JSON.stringify(event.queryStringParameters)}`
      }, null, ' ')
    })
    expect(dynamoDbClient.query).toHaveBeenCalledTimes(0)
  })

  it('returns error when query string is empty', async () => {
    const mockError = 'THIS IS A TEST: Problem creating image Zip'
    // @ts-ignore
    dynamoDbClient.query.mockRejectedValueOnce(mockError)
    const event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.queryStringParameters = undefined
    const res = await lambda.handle(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 401,
      body: JSON.stringify({
        message: `Request is invalid: ${JSON.stringify(event.queryStringParameters)}`
      }, null, ' ')
    })
    expect(dynamoDbClient.query).toHaveBeenCalledTimes(0)
  })
})
