import * as lambda from '../../../src/lambda/cerebrum-image-search'
import { APIGatewayProxyEventV2, Context } from 'aws-lambda'
import merge from 'lodash.merge'
import { dynamoDbClient } from '@exsoinn/aws-sdk-wrappers'
import { ages, agesOutput, diagnoses, diagnosesOutput } from '../../fixture/cerebrum-image-dynamodb-search-result.fixture'

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
    const res = await lambda.search(event, {} as Context, jest.fn())
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
    const res = await lambda.search(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 500,
      body: JSON.stringify({
        message: `Something went wrong, ${mockError}`
      }, null, ' ')
    })
  })

  it('returns error when no search criteria has been specified', async () => {
    const mockError = 'THIS IS A TEST: Problem creating image Zip'
    // @ts-ignore
    dynamoDbClient.query.mockRejectedValueOnce(mockError)
    const event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.queryStringParameters = {
      foo: 'bar'
    }
    const res = await lambda.search(event, {} as Context, jest.fn())
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
    const res = await lambda.search(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 401,
      body: JSON.stringify({
        message: `Request is invalid: ${JSON.stringify(event.queryStringParameters)}`
      }, null, ' ')
    })
    expect(dynamoDbClient.query).toHaveBeenCalledTimes(0)
  })

  it('returns 404 if no results found for a dimension', async () => {
    // @ts-ignore
    dynamoDbClient.scan.mockResolvedValueOnce({})
    const event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.pathParameters = {
      dimension: 'ages'
    }
    const res = await lambda.dimension(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 404,
      body: JSON.stringify([])
    })
    expect(dynamoDbClient.scan).toHaveBeenCalledWith({
      ExpressionAttributeValues: {
        ':true': 'true'
      },
      ExpressionAttributeNames: {
        '#dimension': 'age',
        '#enabled': 'enabled'
      },
      FilterExpression: '#enabled = :true',
      ProjectionExpression: '#dimension',
      TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string,
      IndexName: 'ageIndex'
    })
  })

  it('calculates correctly ranges for dimensions that are numeric', async () => {
    // @ts-ignore
    dynamoDbClient.scan.mockResolvedValueOnce(ages)
    const event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.pathParameters = {
      dimension: 'ages'
    }
    event.queryStringParameters = {
      numeric: 'true'
    }
    const res = await lambda.dimension(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 200,
      body: JSON.stringify(agesOutput, null, ' ')
    })
    expect(dynamoDbClient.scan).toHaveBeenCalledWith({
      ExpressionAttributeValues: {
        ':true': 'true'
      },
      ExpressionAttributeNames: {
        '#dimension': 'age',
        '#enabled': 'enabled'
      },
      FilterExpression: '#enabled = :true',
      ProjectionExpression: '#dimension',
      TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string,
      IndexName: 'ageIndex'
    })
  })

  it('calculates correctly search results are for dimensions that are not numeric', async () => {
    // @ts-ignore
    dynamoDbClient.scan.mockResolvedValueOnce(diagnoses)
    const event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.pathParameters = {
      dimension: 'diagnoses'
    }
    event.queryStringParameters = {
      numeric: 'true'
    }
    const res = await lambda.dimension(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 200,
      body: JSON.stringify(diagnosesOutput, null, ' ')
    })
    expect(dynamoDbClient.scan).toHaveBeenCalledWith({
      ExpressionAttributeValues: {
        ':true': 'true'
      },
      ExpressionAttributeNames: {
        '#dimension': 'diagnosis',
        '#enabled': 'enabled'
      },
      FilterExpression: '#enabled = :true',
      ProjectionExpression: '#dimension',
      TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string,
      IndexName: 'diagnosisIndex'
    })
  })
})
