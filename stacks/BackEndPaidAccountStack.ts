import * as sst from 'sst/constructs'
import * as iam from 'aws-cdk-lib/aws-iam'
import { Bucket as S3Bucket, EventType } from 'aws-cdk-lib/aws-s3'
import * as s3Notifications from 'aws-cdk-lib/aws-s3-notifications'
import { StringAttribute } from 'aws-cdk-lib/aws-cognito'
import { Certificate } from 'aws-cdk-lib/aws-certificatemanager'
import * as route53 from 'aws-cdk-lib/aws-route53'
import { Duration } from 'aws-cdk-lib'
import { Construct } from 'constructs'

/**
 * This stack defines the Charcot backend AWS paid account of Mt Sinai portion of the app
 */
export function BackEndPaidAccountStack({ stack }: sst.StackContext) {
  const stage = stack.stage

  // Auth
  const auth = cognitoUserPool(stack)

  /*
   * SQS Queue(s)
   */
  const cerebrumImageOrderQueue = new sst.Queue(stack, 'cerebrum-image-order-queue', {
    cdk: {
      queue: {
        // Give maximum of 12 hours to process a request, some of them can be large and in fact
        // can take longer then 12 hours. See the fulfillment module for notes on how we
        // handle request which take longer then maximum to process.
        visibilityTimeout: Duration.hours(12),
        receiveMessageWaitTime: Duration.seconds(20)
      }
    }
  })

  /*
   * DynamoDB Tables
   */
  const cerebrumImageMetaDataTable = new sst.Table(stack, 'cerebrum-image-metadata', {
    fields: {
      fileName: 'string',
      region: 'string',
      allSubjectRegions: 'string',
      stain: 'string',
      allSubjectStains: 'string',
      age: 'number',
      race: 'string',
      sex: 'string',
      diagnosis: 'string',
      subjectNumber: 'number',
      uploadDate: 'string',
      enabled: 'string' // This should be a boolean, but alas, SST supports only string/binary/number, not sure why: https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.aws_dynamodb.AttributeType.html
    },
    primaryIndex: { partitionKey: 'fileName' },
    // TODO: Are these indices needed? Nowhere in code is it querying indexes
    globalIndexes: {
      regionIndex: { partitionKey: 'region' },
      stainIndex: { partitionKey: 'stain' },
      ageIndex: { partitionKey: 'age' },
      raceIndex: { partitionKey: 'race' },
      sexIndex: { partitionKey: 'sex' },
      diagnosisIndex: { partitionKey: 'diagnosis' },
      subjectNumberIndex: { partitionKey: 'subjectNumber' }
    }

  })

  const cerebrumImageOrderTable = new sst.Table(stack, 'cerebrum-image-order', {
    fields: {
      orderId: 'string',
      recordNumber: 'number',
      email: 'string',
      created: 'number',
      filter: 'string',
      status: 'string',
      fulfilled: 'number',
      remark: 'string',
      sqsReceiptHandle: 'string',
      size: 'number',
      fileCount: 'number'
    },
    primaryIndex: { partitionKey: 'orderId', sortKey: 'recordNumber' },
    // TODO: Is the 'created' ts index needed???
    globalIndexes: {
      createdIndex: { partitionKey: 'created' }
    }

  })

  /*
   * TODO: IMAGE_TRANSFER_DISABLE: Add feature flag to toggle off image transfer functionality because it is no
   *  longer needed. This will make the paid account "nbtr-odp-staging-<stage>" (source) and
   *  "nbtr-production-<stage>" (target, created in paid account for non-prod envs only and for testing purposes)
   *  buckets go away.
   */
  // Mt Sinai had no concept of stages prior to Charcot, so need the below for backward compatibility
  // with their stage unaware S3 buckets which were in place already before Charcot. Renaming
  // those existing buckets is not an option
  const bucketSuffix = stage === 'prod' ? '' : `-${stage}`
  const cerebrumImageTransferSourceBucketName = `nbtr-odp-staging${bucketSuffix}` // source s3 bucket
  const cerebrumImageOdpBucketName = `nbtr-production${bucketSuffix}` // target s3 bucket

  // Buckets and notification target functions
  const handleCerebrumImageTransfer = new sst.Function(stack, 'HandleCerebrumImageTransfer', {
    functionName: `handle-cerebrum-image-transfer-${stage}`,
    handler: 'src/lambda/cerebrum-image-transfer.handle',
    memorySize: 128,
    initialPolicy: [
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ['s3:GetObject', 's3:DeleteObject'],
        resources: [`arn:aws:s3:::${cerebrumImageTransferSourceBucketName}/*`]
      }),
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ['s3:PutObject'],
        resources: [`arn:aws:s3:::${cerebrumImageOdpBucketName}/*`]
      })
    ],
    environment: {
      CEREBRUM_IMAGE_ODP_BUCKET_NAME: cerebrumImageOdpBucketName
    },
    timeout: 900
  })

  if (stage === 'prod') {
    /*
       * In 'prod' stage bucket was already there before inception
       * of Charcot, so have to work with what was there already (I.e.
       * unable to drop and recreate it)
       */
    const loadedBucket = S3Bucket.fromBucketName(stack, 'BucketLoadedByName', cerebrumImageTransferSourceBucketName)
    loadedBucket.addEventNotification(EventType.OBJECT_CREATED, new s3Notifications.LambdaDestination(handleCerebrumImageTransfer))
  } else {
    new sst.Bucket(stack, cerebrumImageTransferSourceBucketName, {
      name: cerebrumImageTransferSourceBucketName,
      notifications: {
        myNotification: {
          type: 'function',
          function: handleCerebrumImageTransfer,
          events: ['object_created']
        }
      }
    }).attachPermissions(['s3'])
  }

  // Create an HTTP API
  const hostedZone = route53.HostedZone.fromHostedZoneAttributes(stack, 'HostedZone', {
    hostedZoneId: 'Z0341163303ASZWMW1YTS',
    zoneName: 'mountsinaicharcot.org'
  })
  /*
     * Note: W/o explicitly passing in hostedZone, was getting:
     *   'It seems you are configuring custom domains for you URL. And SST is not able to find the hosted zone "mountsinaicharcot.org" in your AWS Route 53 account. Please double check and make sure the zone exists, or pass in a different zone.'
     */
  const api = new sst.Api(stack, 'Api', {
    authorizers: {
      jwt: {
        type: 'user_pool',
        userPool: {
          id: auth.userPoolId,
          clientIds: [auth.userPoolClientId]
        }
      }
    },
    customDomain: {
      domainName: `${stage === 'prod' ? 'api.mountsinaicharcot.org' : `api-${stage}.mountsinaicharcot.org`}`,
      cdk: {
        hostedZone,
        certificate: Certificate.fromCertificateArn(stack, 'MyCert', 'arn:aws:acm:us-east-1:045387143127:certificate/1004f57f-a544-476d-8a31-5b878a71c276')
      }
    },
    routes: {
      'POST /cerebrum-images': {
        authorizer: 'jwt',
        function: {
          functionName: `create-cerebrum-image-metadata-${stage}`,
          handler: 'src/lambda/cerebrum-image-metadata.create',
          initialPolicy: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['dynamodb:PutItem'],
              resources: [cerebrumImageMetaDataTable.tableArn]
            })],
          environment: {
            CEREBRUM_IMAGE_METADATA_TABLE_NAME: cerebrumImageMetaDataTable.tableName
          }
        }
      },
      'GET /cerebrum-images': {
        function: {
          functionName: `handle-cerebrum-image-search-${stage}`,
          handler: 'src/lambda/cerebrum-image-search.search',
          initialPolicy: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['dynamodb:Query', 'dynamodb:Scan'],
              resources: [cerebrumImageMetaDataTable.tableArn, `${cerebrumImageMetaDataTable.tableArn}/index/*`]
            })],
          environment: {
            CEREBRUM_IMAGE_METADATA_TABLE_NAME: cerebrumImageMetaDataTable.tableName
          }
        }
      },
      'GET /cerebrum-images/{dimension}': {
        function: {
          functionName: `handle-cerebrum-image-dimension-${stage}`,
          handler: 'src/lambda/cerebrum-image-search.dimension',
          initialPolicy: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['dynamodb:Query', 'dynamodb:Scan'],
              resources: [cerebrumImageMetaDataTable.tableArn, `${cerebrumImageMetaDataTable.tableArn}/index/*`]
            })],
          environment: {
            CEREBRUM_IMAGE_METADATA_TABLE_NAME: cerebrumImageMetaDataTable.tableName
          }
        }
      },
      'GET /cerebrum-image-users/{email}': {
        authorizer: 'jwt',
        function: {
          functionName: `retrieve-cerebrum-image-user-${stage}`,
          handler: 'src/lambda/cerebrum-image-user.retrieve',
          initialPolicy: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['cognito-idp:AdminGetUser'],
              resources: [auth.userPoolArn]
            })],
          environment: {
            CEREBRUM_COGNITO_USER_POOL_ID: auth.userPoolId
          }
        }
      },
      'PUT /cerebrum-image-users/{email}': {
        authorizer: 'jwt',
        function: {
          functionName: `update-cerebrum-image-user-${stage}`,
          handler: 'src/lambda/cerebrum-image-user.update',
          initialPolicy: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['cognito-idp:AdminUpdateUserAttributes', 'cognito-idp:AdminSetUserPassword'],
              resources: [auth.userPoolArn]
            })],
          environment: {
            CEREBRUM_COGNITO_USER_POOL_ID: auth.userPoolId
          }
        }
      },
      'GET /cerebrum-image-orders': {
        authorizer: 'jwt',
        function: {
          functionName: `retrieve-cerebrum-image-order-${stage}`,
          handler: 'src/lambda/cerebrum-image-order.retrieve',
          initialPolicy: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['dynamodb:Scan'],
              resources: [cerebrumImageOrderTable.tableArn]
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['cognito-idp:AdminGetUser'],
              resources: [auth.userPoolArn]
            })],
          environment: {
            CEREBRUM_IMAGE_ORDER_TABLE_NAME: cerebrumImageOrderTable.tableName,
            CEREBRUM_COGNITO_USER_POOL_ID: auth.userPoolId
          }
        }
      },
      'POST /cerebrum-image-orders': {
        authorizer: 'jwt',
        function: {
          functionName: `create-cerebrum-image-order-${stage}`,
          handler: 'src/lambda/cerebrum-image-order.create',
          initialPolicy: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['dynamodb:PutItem'],
              resources: [cerebrumImageOrderTable.tableArn]
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['dynamodb:Query', 'dynamodb:Scan'],
              resources: [cerebrumImageMetaDataTable.tableArn]
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['sqs:SendMessage'],
              resources: [cerebrumImageOrderQueue.queueArn]
            })],
          environment: {
            CEREBRUM_IMAGE_ORDER_TABLE_NAME: cerebrumImageOrderTable.tableName,
            CEREBRUM_IMAGE_ORDER_QUEUE_URL: cerebrumImageOrderQueue.queueUrl,
            CEREBRUM_IMAGE_METADATA_TABLE_NAME: cerebrumImageMetaDataTable.tableName
          }
        }
      },
      'DELETE /cerebrum-image-orders/{orderId}': {
        authorizer: 'jwt',
        function: {
          functionName: `cancel-cerebrum-image-order-${stage}`,
          handler: 'src/lambda/cerebrum-image-order.cancel',
          initialPolicy: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['dynamodb:UpdateItem', 'dynamodb:GetItem'],
              resources: [cerebrumImageOrderTable.tableArn]
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['cognito-idp:AdminGetUser'],
              resources: [auth.userPoolArn]
            })
          ],
          environment: {
            CEREBRUM_IMAGE_ORDER_TABLE_NAME: cerebrumImageOrderTable.tableName,
            CEREBRUM_COGNITO_USER_POOL_ID: auth.userPoolId
          }
        }
      }
    }
  })

  // TODO: Is this needed? What happens if I were to remove? Would logged in users
  //       be able to hit this endpoint, but not anon ones????? (head scratch)
  //       Experiment,
  //       [REF|https://sst.dev/chapters/adding-auth-to-our-serverless-app.html|"The attachPermissionsForAuthUsers function allows us to specify the resources our authenticated users have access to."]
  auth.attachPermissionsForAuthUsers(stack, [api])

  stack.addOutputs({
    ApiEndpoint: api.customDomainUrl || api.url,
    Region: stack.region,
    UserPoolId: auth.userPoolId,
    CognitoIdentityPoolId: auth.cognitoIdentityPoolId!,
    UserPoolClientId: auth.userPoolClientId,
    HandleCerebrumImageTransferRoleArn: handleCerebrumImageTransfer?.role?.roleArn as string,
    CerebrumImageOrderTableArn: cerebrumImageOrderTable.tableArn,
    CerebrumImageMetadataTableArn: cerebrumImageMetaDataTable.tableArn,
    CerebrumImageOrderQueueArn: cerebrumImageOrderQueue.queueArn,
    CerebrumImageOrderQueueUrl: cerebrumImageOrderQueue.queueUrl,
    CerebrumImageOrderQueueName: cerebrumImageOrderQueue.queueName
  })

  return {
    api,
    handleCerebrumImageTransferRoleArn: handleCerebrumImageTransfer?.role?.roleArn as string,
    userPoolId: auth.userPoolId,
    userPoolClientId: auth.userPoolClientId,
    cognitoIdentityPoolId: auth.cognitoIdentityPoolId,
    cerebrumImageOrderTableArn: cerebrumImageOrderTable.tableArn,
    cerebrumImageMetadataTableArn: cerebrumImageMetaDataTable.tableArn,
    cerebrumImageOrderQueueArn: cerebrumImageOrderQueue.queueArn
  }
}

const cognitoUserPool = (scope: Construct) => new sst.Cognito(scope, 'Auth', {
  login: ['email'],
  cdk: {
    userPoolClient: {
      authFlows: {
        adminUserPassword: true,
        custom: true,
        userSrp: true
      }
    },
    userPool: {
      customAttributes: {
        degree: new StringAttribute({
          minLen: 1,
          maxLen: 256,
          mutable: true
        }),
        institutionName: new StringAttribute({
          minLen: 1,
          maxLen: 256,
          mutable: true
        }),
        institutionAddress: new StringAttribute({
          minLen: 1,
          maxLen: 256,
          mutable: true
        }),
        areasOfInterest: new StringAttribute({
          minLen: 1,
          maxLen: 256,
          mutable: true
        }),
        intendedUse: new StringAttribute({
          minLen: 1,
          maxLen: 500,
          mutable: true
        })
      }
    }
  }
})
