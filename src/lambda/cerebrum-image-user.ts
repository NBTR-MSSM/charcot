import { APIGatewayProxyEventV2, APIGatewayProxyHandlerV2 } from 'aws-lambda'
import { lambdaWrapper } from '@exsoinn/aws-sdk-wrappers'
import userManagement from '../service/user-management'

export const retrieve: APIGatewayProxyHandlerV2 = lambdaWrapper(async (event: APIGatewayProxyEventV2) => {
  return userManagement.retrieve(event)
})

export const update: APIGatewayProxyHandlerV2 = lambdaWrapper(async (event: APIGatewayProxyEventV2) => {
  return userManagement.update(event)
})
