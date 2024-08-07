import { HttpResponse, cognitoIdentityServiceProviderClient } from '@exsoinn/aws-sdk-wrappers'
import { APIGatewayProxyEventV2 } from 'aws-lambda'
import { capitalCase } from 'change-case'
import { CognitoIdentityServiceProvider } from 'aws-sdk'

class UserManagement {
  async retrieve(event: APIGatewayProxyEventV2 | string) {
    let email: string
    if (typeof event === 'string') {
      email = event
    } else {
      email = (event.pathParameters && event.pathParameters.email) as string
    }

    const res = await cognitoIdentityServiceProviderClient.adminGetUser({
      UserPoolId: process.env.CEREBRUM_COGNITO_USER_POOL_ID as string,
      Username: email
    }).promise()

    const user: Record<string, string> = {}
    for (const attr of res.UserAttributes!) {
      const name = attr.Name
      const value = attr.Value
      user[name.replace('custom:', '')] = value as string
      user.firstName = user.given_name
      user.lastName = user.family_name
    }

    // FIXME: Temporary solution to fill in missing name nad last name,
    //   remove once these are populated in cognito user pool
    /* istanbul ignore if */
    if (!user.given_name) {
      const email = user.email as string
      const name = email.substring(0, email.indexOf('@')).split('.')

      if (name && name.length === 2) {
        user.given_name = capitalCase(name[0])
        user.family_name = capitalCase(name[1])
      }
    }
    user.requester = `${user.given_name} ${user.family_name}`

    return new HttpResponse(200, '', {
      body: user
    })
  }

  async update(event: APIGatewayProxyEventV2) {
    const email = (event.pathParameters && event.pathParameters.email) as string
    const updateRequest: Record<string, string> = JSON.parse(event.body!)
    const attributes: CognitoIdentityServiceProvider.AttributeListType = Object.entries(updateRequest).filter(e => e[0] !== 'password').map(e => ({
      Name: e[0],
      Value: e[1] as string
    }))
    await cognitoIdentityServiceProviderClient.adminUpdateUserAttributes({
      UserPoolId: process.env.CEREBRUM_COGNITO_USER_POOL_ID as string,
      Username: email,
      UserAttributes: attributes
    }).promise()

    // Update the password if one was provided
    if (updateRequest.password) {
      await cognitoIdentityServiceProviderClient.adminSetUserPassword({
        UserPoolId: process.env.CEREBRUM_COGNITO_USER_POOL_ID as string,
        Username: email,
        Password: updateRequest.password,
        Permanent: true
      }).promise()
    }
  }
}

export default new UserManagement()
