import * as lambda from '../../../src/lambda/cerebrum-image-fulfillment'
import order from '../../fixture/cerebrum-image-order.fixture'
import { Context } from 'aws-lambda'
import { dynamoDbClient, s3Client, sesClient } from '@exsoinn/aws-sdk-wrappers'
import { CerebrumImageOrder } from '../../../src/types/charcot.types'

const mockTime = '2021-12-27 00:00:00 UTC'
const jestGlobal = global as any
describe('cerebrum-image-fulfillment', () => {
  it('handles image fulfillment', async () => {
    // @ts-ignore
    dynamoDbClient.get.mockResolvedValueOnce({
      Item: order
    })
    const mockZipLink = 'http://www.signed-url.com/path.zip'
    // @ts-ignore
    s3Client.getSignedUrlPromise.mockResolvedValueOnce('http://www.signed-url.com/path.zip')
    const currentTime = new Date(mockTime)
    // @ts-ignore
    const dateSpy = jest.spyOn(global, 'Date').mockImplementation(() => currentTime)
    const res = await lambda.handle({ orderId: jestGlobal.dummyOrderId }, {} as Context, jest.fn())
    expect(res).toBeDefined()
    expect(dynamoDbClient.get).toHaveBeenLastCalledWith({
      TableName: process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME,
      Key: {
        orderId: jestGlobal.dummyOrderId
      }
    })
    expect((s3Client as any).zipObjectsToBucket).toHaveBeenCalledWith(process.env.CEREBRUM_IMAGE_ODP_BUCKET_NAME, '', [order.fileNames[0], 'XE13-009_2_HE_1/', order.fileNames[1], 'XE13-009_2_Sil_1/', order.fileNames[2], 'XE12-025_1_HE_1/'], process.env.CEREBRUM_IMAGE_ZIP_BUCKET_NAME, `zip/${buildZipName(order)}`)
    // @ts-ignore
    expect(sesClient.sendEmail).toHaveBeenCalledWith({
      Destination: {
        ToAddresses: [order.email]
      },
      Message: {
        Body: {
          Html: {
            Data: "Your requested image Zip is ready. You can access via this <a href='http://www.signed-url.com/path.zip'>link</a>"
          },
          Text: { Data: `Your requested image Zip is ready. You can access via this link: ${mockZipLink}.` }
        },
        Subject: { Data: 'Charcot Image Request Ready' }
      },
      Source: 'no-reply@mountsinaicharcot.org'
    })
    dateSpy.mockRestore()
  })

  it('handles unexpected errors', async () => {
    // @ts-ignore
    dynamoDbClient.get.mockResolvedValueOnce({
      Item: order
    })
    const mockError = 'THIS IS A TEST: Problem creating image Zip'
    // @ts-ignore
    s3Client.zipObjectsToBucket.mockRejectedValueOnce(mockError)
    const consoleErrorSpy = jest.spyOn(console, 'error')
    const res = await lambda.handle({ orderId: jestGlobal.dummyOrderId }, {} as Context, jest.fn())
    expect(res).toBeDefined()
    expect(consoleErrorSpy).toHaveBeenCalledWith(`Problem processing order ${JSON.stringify(order)}`, mockError)
  })
})

function buildZipName(order: CerebrumImageOrder): string {
  return `${order.orderId}.zip`
}
