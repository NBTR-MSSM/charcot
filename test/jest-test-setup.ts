import images from './fixture/cerebrum-image-metadata.fixture'
import { APIGatewayProxyEventV2 } from 'aws-lambda'
import { s3Client } from '@exsoinn/aws-sdk-wrappers'

const jestGlobal = global as any
jestGlobal.dummyOrderId = 'abc123'

jest.mock('ioredis')
jest.mock('uuid', () => ({
  v4: () => jestGlobal.dummyOrderId
}))

// Set up mocks for parts of the API we're using the code base,
// and not for everything else.
const awsWrappers = jest.requireActual('@exsoinn/aws-sdk-wrappers')
jest.mock('@exsoinn/aws-sdk-wrappers', () => {
  awsWrappers.dynamoDbClient.get = jest.fn(() => Promise.resolve())
  awsWrappers.dynamoDbClient.put = jest.fn(() => Promise.resolve())
  awsWrappers.dynamoDbClient.query = jest.fn(() => Promise.resolve())
  awsWrappers.lambdaClient.invokeLambda = jest.fn(() => Promise.resolve())
  awsWrappers.s3Client.getSignedUrlPromise = jest.fn(() => Promise.resolve())
  awsWrappers.s3Client.zipObjectsToBucket = jest.fn(() => Promise.resolve())
  awsWrappers.sesClient.sendEmail = jest.fn(() => Promise.resolve())
  return awsWrappers
})

process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME = 'cerebrum-image-metadata'
process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME = 'cerebrum-image-order'
process.env.CEREBRUM_IMAGE_BUCKET_NAME = 'cerebrum-image'
process.env.CEREBRUM_IMAGE_ZIP_BUCKET_NAME = 'cerebrum-image-zip'
process.env.HANDLE_CEREBRUM_IMAGE_FULFILLMENT_FUNCTION_NAME = 'handle-cerebrum-image-request-dev'
process.env.ZIP_LINK_EXPIRY = '999'

// FIXME: By default leave event.body blank, and move image data mocking to that test file
jestGlobal.BASE_REQUEST = {
  headers: { 'content-type': 'application/x-www-form-urlencoded' },
  body: JSON.stringify(images),
  isBase64Encoded: true,
  rawPath: '',
  rawQueryString: '',
  requestContext: {
    accountId: '',
    apiId: '',
    domainName: '',
    domainPrefix: '',
    http: { method: '', path: '', protocol: '', sourceIp: '', userAgent: '' },
    requestId: '',
    routeKey: '',
    stage: '',
    time: '',
    timeEpoch: 0
  },
  routeKey: '',
  version: ''
} as APIGatewayProxyEventV2
