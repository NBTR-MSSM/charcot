import { APIGatewayProxyEventV2, APIGatewayProxyHandlerV2 } from 'aws-lambda'
import { lambdaWrapper } from '@exsoinn/aws-sdk-wrappers'
import imageSearch from '../service/image-search'

export const search: APIGatewayProxyHandlerV2 = lambdaWrapper(async (event: APIGatewayProxyEventV2) => {
  return imageSearch.search(event)
})

export const dimension: APIGatewayProxyHandlerV2 = lambdaWrapper(async (event: APIGatewayProxyEventV2) => {
  return imageSearch.dimension(event)
})
