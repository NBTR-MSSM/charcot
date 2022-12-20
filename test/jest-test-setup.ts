import { APIGatewayProxyEventV2 } from 'aws-lambda'

const jestGlobal = global as any
jestGlobal.dummyOrderId = 'abc123'

jest.mock('ioredis')
jest.mock('uuid', () => ({
  v4: () => jestGlobal.dummyOrderId
}))

// Set up mocks for parts of the API we're using the in our code base,
// and not for everything else.
jest.mock('@exsoinn/aws-sdk-wrappers', () => {
  const awsWrappers = jest.requireActual('@exsoinn/aws-sdk-wrappers')
  awsWrappers.axiosClient.post = jest.fn(() => Promise.resolve())
  awsWrappers.cognitoIdentityServiceProviderClient.adminGetUser = jest.fn(() => ({
    promise: () => Promise.resolve()
  }))
  awsWrappers.cognitoIdentityServiceProviderClient.adminUpdateUserAttributes = jest.fn(() => ({
    promise: () => Promise.resolve()
  }))
  awsWrappers.cognitoIdentityServiceProviderClient.adminSetUserPassword = jest.fn(() => ({
    promise: () => Promise.resolve()
  }))
  awsWrappers.dynamoDbClient.get = jest.fn(() => Promise.resolve())
  awsWrappers.dynamoDbClient.put = jest.fn(() => Promise.resolve())
  awsWrappers.dynamoDbClient.query = jest.fn(() => Promise.resolve())
  awsWrappers.dynamoDbClient.scan = jest.fn(() => Promise.resolve())
  awsWrappers.dynamoDbClient.update = jest.fn(() => Promise.resolve())
  awsWrappers.lambdaClient.invokeLambda = jest.fn(() => Promise.resolve())
  awsWrappers.s3Client.copy = jest.fn(() => Promise.resolve())
  awsWrappers.s3Client.deleteObject = jest.fn(() => Promise.resolve())
  awsWrappers.s3Client.getSignedUrlPromise = jest.fn(() => Promise.resolve())
  awsWrappers.s3Client.zipObjectsToBucket = jest.fn(() => Promise.resolve())
  awsWrappers.s3Client.buildNewClient = jest.fn(() => awsWrappers.s3Client)
  awsWrappers.sesClient.sendEmail = jest.fn(() => ({
    promise: () => Promise.resolve()
  }))
  awsWrappers.sqsClient.send = jest.fn(() => Promise.resolve())
  return awsWrappers
})

process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME = 'cerebrum-image-metadata'
process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME = 'cerebrum-image-order'
process.env.CEREBRUM_IMAGE_ODP_BUCKET_NAME = 'nbtr-production'
process.env.CEREBRUM_IMAGE_ZIP_BUCKET_NAME = 'cerebrum-image-zip'
process.env.ZIP_LINK_EXPIRY = '999'
process.env.FROM_EMAIL = 'no-reply@mountsinaicharcot.org'
process.env.CEREBRUM_IMAGE_ORDER_QUEUE_URL = 'fulfillment.mountsinaicharcot.org/queue'
process.env.CEREBRUM_COGNITO_USER_POOL_ID = 'user-pool-id'

jestGlobal.BASE_REQUEST = {
  headers: { 'content-type': 'application/x-www-form-urlencoded' },
  body: '',
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
