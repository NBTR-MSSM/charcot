import * as lambda from '../../../src/lambda/cerebrum-image-search'
import { APIGatewayProxyEventV2, Context } from 'aws-lambda'
import merge from 'lodash.merge'
import { dynamoDbClient } from '@exsoinn/aws-sdk-wrappers'
import {
  agesScanResult,
  agesOutput,
  agesPaginatedScanResult,
  allFieldsScanResult,
  diagnosesScanResult,
  diagnosesOutput
} from '../../fixture/cerebrum-image-image.fixture'

const jestGlobal = global as unknown as Record<string, string>
describe('cerebrum-image-search', () => {
  beforeEach(() => {
    jest.clearAllMocks()
  })
  it('handles image search', async () => {
    // @ts-ignore
    dynamoDbClient.scan.mockResolvedValueOnce(allFieldsScanResult)
    const event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.queryStringParameters = {
      filter: 'age = \'90+\''
    }
    const res = await lambda.search(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(allFieldsScanResult.Items, null, ' ')
    })
    expect(dynamoDbClient.scan).toHaveBeenCalledWith({
      TableName: 'cerebrum-image-metadata',
      FilterExpression: '#age >= :90',
      ExpressionAttributeNames: {
        '#age': 'age'
      },
      ExpressionAttributeValues: {
        ':90': 90
      }
    })
  })

  it('paginates image search results', async () => {
    // @ts-ignore
    dynamoDbClient.scan.mockResolvedValueOnce(allFieldsScanResult)
    const event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.queryStringParameters = {
      filter: 'age = \'90+\'',
      page: '1',
      pageSize: '2'
    }
    const res = await lambda.search(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(allFieldsScanResult.Items!.slice(0, 2), null, ' ')
    })
    expect(dynamoDbClient.scan).toHaveBeenCalledWith({
      TableName: 'cerebrum-image-metadata',
      FilterExpression: '#age >= :90',
      ExpressionAttributeNames: {
        '#age': 'age'
      },
      ExpressionAttributeValues: {
        ':90': 90
      }
    })
  })

  it('handles case when no results are found for a dimension', async () => {
    // @ts-ignore
    dynamoDbClient.scan.mockResolvedValueOnce({})
    const event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.pathParameters = {
      dimension: 'ages'
    }
    const res = await lambda.dimension(event, {} as Context, jest.fn())
    expect(res).toEqual({
      statusCode: 200,
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
      TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string
    })
  })

  it('calculates correctly ranges for dimensions that are numeric', async () => {
    // @ts-ignore
    dynamoDbClient.scan.mockResolvedValueOnce(agesScanResult)
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
      TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string
    })
  })

  it('calculates correctly search results are for dimensions that are not numeric', async () => {
    // @ts-ignore
    dynamoDbClient.scan.mockResolvedValueOnce(diagnosesScanResult)
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
      TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string
    })
  })

  it('applies filter correctly', async () => {
    const event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.pathParameters = {
      dimension: 'regions'
    }
    event.queryStringParameters = {
      filter: '(age = \'84 - 89\' OR age = \'90+\' OR age = \'< 10\') AND diagnosis = \'Probable Alzheimer__QUOTE__s Disease\' AND subjectNumber = \'12345\' AND contains(stain, \'myl\')'
    }
    await lambda.dimension(event, {} as Context, jest.fn())
    expect(dynamoDbClient.scan).toHaveBeenCalledWith({
      ExpressionAttributeValues: {
        ':84': 84,
        ':89': 89,
        ':90': 90,
        ':10': 10,
        ':ProbableAlzheimer__QUOTE__sDisease': 'Probable Alzheimer\'s Disease',
        ':12345': 12345,
        ':true': 'true',
        ':myl': 'myl'
      },
      ExpressionAttributeNames: {
        '#age': 'age',
        '#dimension': 'region',
        '#diagnosis': 'diagnosis',
        '#enabled': 'enabled',
        '#stain': 'stain',
        '#subjectNumber': 'subjectNumber'
      },
      FilterExpression: '(#age BETWEEN :84 AND :89 OR #age >= :90 OR #age < :10) AND #diagnosis = :ProbableAlzheimer__QUOTE__sDisease AND #subjectNumber = :12345 AND contains(#stain, :myl) AND #enabled = :true',
      ProjectionExpression: '#dimension',
      TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string
    })
  })

  it('handles dynamodb pagination correctly', async () => {
    // @ts-ignore
    dynamoDbClient.scan.mockResolvedValueOnce(agesPaginatedScanResult)
    // @ts-ignore
    dynamoDbClient.scan.mockResolvedValueOnce(agesScanResult)
    const event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.pathParameters = {
      dimension: 'ages'
    }
    event.queryStringParameters = {
      numeric: 'true'
    }
    await lambda.dimension(event, {} as Context, jest.fn())
    const expectedScanParams = {
      ExpressionAttributeValues: {
        ':true': 'true'
      },
      ExpressionAttributeNames: {
        '#dimension': 'age',
        '#enabled': 'enabled'
      },
      FilterExpression: '#enabled = :true',
      ProjectionExpression: '#dimension',
      TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string
    }
    expect(dynamoDbClient.scan).toHaveBeenNthCalledWith(1, expectedScanParams)
    expect(dynamoDbClient.scan).toHaveBeenNthCalledWith(2, {
      ...expectedScanParams,
      ExclusiveStartKey: {
        foo: 'bar'
      }
    })
  })
})
