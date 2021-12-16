import * as lambda from '../../../src/lambda/cerebrum-image-request-processor'
import request from '../../fixture/cerebrum-image-request.fixture'
import { Context } from 'aws-lambda'
import { s3Client } from '@exsoinn/aws-sdk-wrappers'
import { CerebrumImageRequest } from '../../../src/types/charcot.types'

const mockTime = '2021-12-27 00:00:00 UTC'
describe('cerebrum-image-request-processor', () => {
  it('handles image request', async () => {
    const currentTime = new Date(mockTime)
    // @ts-ignore
    const dateSpy = jest.spyOn(global, 'Date').mockImplementation(() => currentTime)
    const res = await lambda.handle(request, {} as Context)
    expect(res).toBeDefined()
    expect(s3Client.zipObjectsToBucket).toHaveBeenCalledWith(process.env.CEREBRUM_IMAGE_BUCKET_NAME, 'image/', request.fileNames, process.env.CEREBRUM_IMAGE_ZIP_BUCKET_NAME, `zip/${buildZipName(request)}`)
    dateSpy.mockRestore()
  })

  it('handles unexpected errors', async () => {
    const mockError = 'THIS IS A TEST: Problem creating image Zip'
    // @ts-ignore
    s3Client.zipObjectsToBucket.mockRejectedValueOnce(mockError)
    const consoleErrorSpy = jest.spyOn(console, 'error')
    const res = await lambda.handle(request, {} as Context)
    expect(res).toBeDefined()
    expect(consoleErrorSpy).toHaveBeenCalledWith(`Problem processing request ${JSON.stringify(request)}`, mockError)
  })
})

function buildZipName(request: CerebrumImageRequest): string {
  return `${request.requestorEmail.replace(/\W/g, '-')}-${mockTime.replace(' UTC', '').replace(/\s|:/g, '-')}.zip`
}
