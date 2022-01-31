import * as lambda from '../../../src/lambda/cerebrum-image-transfer'
import { Context } from 'aws-lambda'
import s3CreateEvent from '../../fixture/cerebrum-image-s3-create-event.fixture'
import { s3Client } from '@exsoinn/aws-sdk-wrappers'

describe('cerebrum-image-transfer', () => {
  beforeEach(() => {
    jest.clearAllMocks()
  })
  it('transfers images', async () => {
    await lambda.handle(s3CreateEvent, {} as Context, jest.fn())
    const s3Info = s3CreateEvent.Records[0].s3
    // @ts-ignore
    expect(s3Client.copy).toHaveBeenCalledWith(s3Info.bucket.name, s3Info.object.key, process.env.CEREBRUM_IMAGE_ODP_BUCKET_NAME, s3Info.object.key)
    expect(s3Client.deleteObject).toHaveBeenCalledWith({
      Bucket: s3Info.bucket.name,
      Key: s3Info.object.key
    })
  })
})
