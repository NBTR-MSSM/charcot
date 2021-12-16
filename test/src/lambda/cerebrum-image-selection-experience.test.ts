import * as lambda from '../../../src/lambda/cerebrum-image-selection-experience'
import { APIGatewayProxyEventV2, Context } from 'aws-lambda'
import merge from 'lodash.merge'
import { dynamoDbClient, lambdaClient } from '@exsoinn/aws-sdk-wrappers'

const imageProcessorLambdaName = process.env.HANDLE_CEREBRUM_IMAGE_REQUEST_FUNCTION_NAME as string

const jestGlobal = global as any
describe('cerebrum-image-selection-experience', () => {
  beforeEach(() => {
    jest.clearAllMocks()
  })
  it('handles image shopping experience', async () => {
    const currentTime = new Date('2021-12-27 00:00:00 UTC')
    // @ts-ignore
    const dateSpy = jest.spyOn(global, 'Date').mockImplementation(() => currentTime)
    const files = ['file1', 'file2', 'file3']
    // @ts-ignore
    dynamoDbClient.query.mockResolvedValueOnce({
      ConsumedCapacity: {},
      Count: 3,
      Items: files.map(f => (
        {
          fileName: f
        }))
    })
    // @ts-ignore
    lambdaClient.invokeLambda.mockResolvedValueOnce({
      StatusCode: 202
    })
    const event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.queryStringParameters = {
      sex: 'Male',
      race: 'Black',
      stain: 'XYZ',
      requestorEmail: 'john.smith@acme.com'
    }
    const res = await lambda.handle(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 202,
      body: JSON.stringify({
        message: 'Your request is being processed, you will get an email soon'
      }, null, ' ')
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
      ProjectionExpression: 'fileName',
      KeyConditionExpression: '#sex = :sex',
      FilterExpression: '#stain = :stain AND #race = :race'
    })

    // @ts-ignore
    expect(lambdaClient.invokeLambda.mock.calls[0][0]).toEqual(imageProcessorLambdaName)
    // @ts-ignore
    expect(lambdaClient.invokeLambda.mock.calls[0][1]).toEqual('Event')
    // @ts-ignore
    expect(JSON.parse(lambdaClient.invokeLambda.mock.calls[0][2])).toEqual({
      requestorEmail: 'john.smith@acme.com',
      fileNames: files,
      created: '2021-12-27T00:00:00.000Z'
    })
    // @ts-ignore
    expect(lambdaClient.invokeLambda.mock.calls[0][3]).toEqual(imageProcessorLambdaName)
    dateSpy.mockRestore()
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
      requestorEmail: 'john.smith@acme.com'
    }
    const res = await lambda.handle(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 500,
      body: JSON.stringify({
        message: `Something went wrong, ${mockError}`
      }, null, ' ')
    })
    expect(lambdaClient.invokeLambda).toHaveBeenCalledTimes(0)
  })

  it('returns error when email is missing', async () => {
    const mockError = 'THIS IS A TEST: Problem creating image Zip'
    // @ts-ignore
    dynamoDbClient.query.mockRejectedValueOnce(mockError)
    const event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.queryStringParameters = {
      sex: 'Male',
      race: 'Black',
      stain: 'XYZ'
    }
    const res = await lambda.handle(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 401,
      body: JSON.stringify({
        message: `Request is invalid: ${JSON.stringify(event.queryStringParameters)}`
      }, null, ' ')
    })
    expect(lambdaClient.invokeLambda).toHaveBeenCalledTimes(0)
  })

  it('returns error no search criteria has been specified', async () => {
    const mockError = 'THIS IS A TEST: Problem creating image Zip'
    // @ts-ignore
    dynamoDbClient.query.mockRejectedValueOnce(mockError)
    const event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.queryStringParameters = {
      requestorEmail: 'john.smith@acme.com'
    }
    const res = await lambda.handle(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 401,
      body: JSON.stringify({
        message: `Request is invalid: ${JSON.stringify(event.queryStringParameters)}`
      }, null, ' ')
    })
    expect(lambdaClient.invokeLambda).toHaveBeenCalledTimes(0)
  })
})
