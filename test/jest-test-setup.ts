import images from './fixture/cerebrum-image-metadata.fixture'
import { APIGatewayProxyEventV2 } from 'aws-lambda'

jest.mock('ioredis')

const jestGlobal = global as any

// Set up mocks for parts of the API we're using the code base,
// and not for everything else.
const awsWrappers = jest.requireActual('@exsoinn/aws-sdk-wrappers')
jest.mock('@exsoinn/aws-sdk-wrappers', () => {
  awsWrappers.dynamoDbClient.put = jest.fn(() => Promise.resolve())
  awsWrappers.dynamoDbClient.query = jest.fn(() => Promise.resolve())
  awsWrappers.lambdaClient.invokeLambda = jest.fn(() => Promise.resolve())
  awsWrappers.s3Client.zipObjectsToBucket = jest.fn(() => Promise.resolve())
  return awsWrappers
})

process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME = 'cerebrum-image-metadata'
process.env.CEREBRUM_IMAGE_BUCKET_NAME = 'cerebrum-image'
process.env.CEREBRUM_IMAGE_ZIP_BUCKET_NAME = 'cerebrum-image-zip'
process.env.HANDLE_CEREBRUM_IMAGE_REQUEST_FUNCTION_NAME = 'handle-cerebrum-image-request-dev'

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
