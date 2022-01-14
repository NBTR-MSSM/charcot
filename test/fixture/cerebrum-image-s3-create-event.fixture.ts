import { S3Event } from 'aws-lambda/trigger/s3'

export default {
  Records: [
    {
      eventVersion: '2.1',
      eventSource: 'aws:s3',
      awsRegion: 'us-east-1',
      eventTime: '2022-01-15T05:56:18.242Z',
      eventName: 'ObjectCreated:Put',
      userIdentity: {
        principalId: 'AWS:AIDAIUVHJK7WNAHYM2ENW'
      },
      requestParameters: {
        sourceIPAddress: '170.20.11.0'
      },
      responseElements: {
        'x-amz-request-id': '83KRB7RM669R5PRX',
        'x-amz-id-2': 'GN+5RdUiZ4m53uXFZoY4EhEYmgmO8UOVPMgqItTwQrU6ZETlfgQGGqpTypPJEpM47DJwVpMJctbn4T820S4gmkvVhaz7FlT9'
      },
      s3: {
        s3SchemaVersion: '1.0',
        configurationId: 'OGY3NmIyNjgtOGNlMy00MzlhLWFjOTEtYWE0ODJiZjY4Nzc1',
        bucket: {
          name: 'cerebrum-image-dev',
          ownerIdentity: {
            principalId: 'A3RJ8QJ29SVTH7'
          },
          arn: 'arn:aws:s3:::cerebrum-image-dev'
        },
        object: {
          key: 'image/gsstanim.gif',
          size: 932818,
          eTag: '9d0f12db58df3f3c7d45008b1b4088f8',
          sequencer: '0061E262003326AAD8'
        }
      }
    }
  ]
} as S3Event
