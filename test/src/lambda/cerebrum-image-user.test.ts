import * as lambda from '../../../src/lambda/cerebrum-image-user'
import { APIGatewayProxyEventV2, APIGatewayProxyStructuredResultV2, Context } from 'aws-lambda'
import merge from 'lodash.merge'
import { cognitoIdentityServiceProviderClient } from '@exsoinn/aws-sdk-wrappers'
import { updateRequestFactory, userFactory } from '../../fixture/cerebrum-image-user.fixture'

const jestGlobal = global as unknown as Record<string, string>
let event: APIGatewayProxyEventV2
describe('cerebrum-image-user', () => {
  beforeEach(() => {
    jest.clearAllMocks()
    event = {} as APIGatewayProxyEventV2
    merge(event, jestGlobal.BASE_REQUEST)
  })

  it('retrieves user', async () => {
    // @ts-ignore
    cognitoIdentityServiceProviderClient.adminGetUser.mockImplementationOnce((params: cognitoIdentityServiceProviderClient.AdminGetUserRequest) => {
      const user = userFactory()
      user.Username = params.Username
      return { promise: () => Promise.resolve(user) }
    })
    event.pathParameters = {
      email: 'test@test.com'
    }

    const res = await lambda.retrieve(event, {} as Context, jest.fn()) as APIGatewayProxyStructuredResultV2
    expect(res).toEqual({
      statusCode: 200,
      body: JSON.stringify({
        institutionAddress: '123 Main St\nAmherst, MA 01002',
        firstName: 'Jose',
        lastName: 'Quijada',
        sub: 'b02712c7-ff3b-4b3d-975a-c1b91a408519',
        email_verified: 'true',
        institutionName: 'University of Massachusetts',
        areasOfInterest: 'Research',
        intendedUse: 'Test desc',
        given_name: 'Jose',
        degree: 'BS',
        family_name: 'Quijada',
        email: 'john.doe@gmail.com',
        requester: 'Jose Quijada'
      }, null, ' ')
    })
  })

  it('updates user', async () => {
    event.pathParameters = {
      email: 'test@test.com'
    }
    const updateRequest = updateRequestFactory()

    // throw password in the request, should get ignored, hence wil be absent in the UserAttributes passed
    // to adminUpdateUserAttributes() call.
    updateRequest.password = 'thePassword'

    event.body = JSON.stringify(updateRequest)
    const res = await lambda.update(event, {} as Context, jest.fn()) as APIGatewayProxyStructuredResultV2
    expect(cognitoIdentityServiceProviderClient.adminUpdateUserAttributes).toHaveBeenCalledWith({
      UserPoolId: process.env.CEREBRUM_COGNITO_USER_POOL_ID as string,
      Username: 'test@test.com',
      UserAttributes: userFactory().UserAttributes
    })
    expect(res).toEqual({
      statusCode: 200,
      body: JSON.stringify({
        message: 'Operation successful'
      }, null, ' ')
    })
  })

  it('updates password', async () => {
    event.pathParameters = {
      email: 'test@test.com'
    }
    const updateRequest = updateRequestFactory()

    // throw password in the request, should get ignored, hence wil be absent in the UserAttributes passed
    // to adminUpdateUserAttributes() call.
    updateRequest.password = 'thePassword'

    event.body = JSON.stringify(updateRequest)
    const res = await lambda.update(event, {} as Context, jest.fn()) as APIGatewayProxyStructuredResultV2
    expect(cognitoIdentityServiceProviderClient.adminSetUserPassword).toHaveBeenCalledWith({
      UserPoolId: process.env.CEREBRUM_COGNITO_USER_POOL_ID as string,
      Username: 'test@test.com',
      Password: updateRequest.password,
      Permanent: true
    })
    expect(cognitoIdentityServiceProviderClient.adminUpdateUserAttributes).toHaveBeenCalledWith({
      UserPoolId: process.env.CEREBRUM_COGNITO_USER_POOL_ID as string,
      Username: 'test@test.com',
      UserAttributes: userFactory().UserAttributes
    })
    expect(res).toEqual({
      statusCode: 200,
      body: JSON.stringify({
        message: 'Operation successful'
      }, null, ' ')
    })
  })
})
