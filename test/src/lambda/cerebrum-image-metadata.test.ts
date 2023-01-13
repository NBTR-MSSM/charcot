import { dynamoDbClient } from '@exsoinn/aws-sdk-wrappers'
import images from '../../fixture/cerebrum-image-metadata.fixture'
import * as lambda from '../../../src/lambda/cerebrum-image-metadata'
import { APIGatewayProxyEventV2, Context } from 'aws-lambda'
import merge from 'lodash.merge'

const jestGlobal = global as unknown as Record<string, string>
let event: APIGatewayProxyEventV2
describe('cerebrum-image-metadata', () => {
  beforeEach(() => {
    jest.clearAllMocks()
    event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
    event.body = JSON.stringify(images)
  })
  it('creates cerebrum image metadata', async () => {
    const res = await lambda.create(event, {} as Context, jest.fn())
    validateImagesWereProcessed()
    expect(res).toEqual(successMessage)
  })

  it('returns error on the first image metadata that fails', async () => {
    // @ts-ignore
    dynamoDbClient.put.mockRejectedValueOnce('THIS IS A TEST: Problem processing image')
    const res = await lambda.create(event, {} as Context, jest.fn())
    expect(res).toEqual(errorMessage)
  })

  it('image data processed sequentially by imageNumber', async () => {
    // scramble the order of the images; lambda should still process them in order of imageNumber (lowest to greatest)
    event.body = JSON.stringify([images[2], images[0], images[1]])
    const res = await lambda.create(event, {} as Context, jest.fn())
    validateImagesWereProcessed()
    expect(res).toEqual(successMessage)
  })
})

const successMessage = {
  statusCode: 200,
  body: JSON.stringify({
    message: 'Request processed successfully'
  }, null, ' ')
}

const errorMessage = {
  statusCode: 400,
  body: JSON.stringify({
    message: 'Problem processing image data: THIS IS A TEST: Problem processing image'
  }, null, ' ')
}

function validateImagesWereProcessed() {
  // @ts-ignore
  expect(dynamoDbClient.put.mock.calls[0][0]).toEqual(
    {
      TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME,
      Item: images[0]
    })
  // @ts-ignore
  expect(dynamoDbClient.put.mock.calls[1][0]).toEqual({
    TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME,
    Item: images[1]
  })
  // @ts-ignore
  expect(dynamoDbClient.put.mock.calls[2][0]).toEqual({
    TableName: process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME,
    Item: images[2]
  })
}
