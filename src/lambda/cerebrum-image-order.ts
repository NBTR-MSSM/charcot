import { APIGatewayProxyEventV2, APIGatewayProxyHandlerV2 } from 'aws-lambda'
import { lambdaWrapper } from '@exsoinn/aws-sdk-wrappers'
import orderSearch from '../service/order-search'
import orderManagement from '../service/order-management'

export const retrieve: APIGatewayProxyHandlerV2 = lambdaWrapper(async (event: APIGatewayProxyEventV2) => {
  return orderSearch.retrieve(event)
})

export const create: APIGatewayProxyHandlerV2 = lambdaWrapper(async (event: APIGatewayProxyEventV2) => {
  return orderManagement.create(event)
})

export const cancel: APIGatewayProxyHandlerV2 = lambdaWrapper(async (event: APIGatewayProxyEventV2) => {
  return orderManagement.cancel(event)
})
