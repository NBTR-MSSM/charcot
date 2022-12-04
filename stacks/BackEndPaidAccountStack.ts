import * as sst from '@serverless-stack/resources'
import { Auth, Bucket } from '@serverless-stack/resources'
import * as iam from 'aws-cdk-lib/aws-iam'
import { Bucket as S3Bucket, EventType } from 'aws-cdk-lib/aws-s3'
import * as s3Notifications from 'aws-cdk-lib/aws-s3-notifications'
import { StackArguments } from '../src/types/charcot.types'
import { StringAttribute } from 'aws-cdk-lib/aws-cognito'
import { Certificate } from 'aws-cdk-lib/aws-certificatemanager'
import * as route53 from 'aws-cdk-lib/aws-route53'
import { Duration } from 'aws-cdk-lib'
import { Construct } from 'constructs'

/**
 * This stack defines the Charcot backend AWS paid account of Mt Sinai portion of the app
 */
export default class BackEndPaidAccountStack extends sst.Stack {
  api: sst.Api
  handleCerebrumImageTransferRoleArn: string
  userPoolId: string
  userPoolClientId: string
  cognitoIdentityPoolId?: string
  cerebrumImageOrderTableArn: string
  cerebrumImageMetadataTableArn: string
  cerebrumImageOrderQueueArn: string

  constructor(scope: sst.App, id: string, props: sst.StackProps, args: StackArguments) {
    super(scope, id, props)

    const stage = this.stage

    // Auth
    const auth = cognitoUserPool(this)

    /*
     * SQS Queue(s)
     */
    const cerebrumImageOrderQueue = new sst.Queue(this, process.env.CEREBRUM_IMAGE_ORDER_QUEUE_NAME as string, {
      cdk: {
        queue: {
          // Give maximum of 12 hours to process a request, some of them can be large and in fact
          // can take ponger then 12 hours. See the fulfillment module for notes on how we
          // handle request which take longer then maximum to process.
          visibilityTimeout: Duration.hours(12),
          receiveMessageWaitTime: Duration.seconds(20)
        }
      }
    })

    /*
     * DynamoDB Tables
     */
    const cerebrumImageMetaDataTable = new sst.Table(this, process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string, {
      fields: {
        fileName: 'string',
        region: 'string',
        stain: 'string',
        age: 'number',
        race: 'string',
        sex: 'string',
        diagnosis: 'string',
        subjectNumber: 'number',
        uploadDate: 'string',
        enabled: 'string'
      },
      primaryIndex: { partitionKey: 'fileName' },
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

    const cerebrumImageOrderTable = new sst.Table(this, process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME as string, {
      fields: {
        orderId: 'string',
        email: 'string',
        created: 'number',
        filter: 'string',
        status: 'string',
        fulfilled: 'number',
        remark: 'string',
        sqsReceiptHandle: 'string',
        size: 'number'
      },
      primaryIndex: { partitionKey: 'orderId' },
      // TODO: Is the 'created' ts index needed???
      globalIndexes: {
        createdIndex: { partitionKey: 'created' }
      }
    })

    // Mt Sinai had no concept of stages prior to Charcot, so need the below for backward compatibility
    // with their stage-less S3 buckets which were in place already before Charcot. Renaming
    // those existing buckets is not an option
    const bucketSuffix = stage === 'prod' ? '' : `-${stage}`
    const cerebrumImageBucketName = `${process.env.CEREBRUM_IMAGE_BUCKET_NAME}${bucketSuffix}` // source s3 bucket
    const cerebrumImageOdpBucketName = `${process.env.CEREBRUM_IMAGE_ODP_BUCKET_NAME}${bucketSuffix}` // target s3 bucket

    // Buckets and notification target functions
    const handleCerebrumImageTransfer = new sst.Function(this, 'HandleCerebrumImageTransfer', {
      functionName: `handle-cerebrum-image-transfer-${stage}`,
      handler: 'src/lambda/cerebrum-image-transfer.handle',
      memorySize: 128,
      initialPolicy: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: ['s3:GetObject', 's3:DeleteObject'],
          resources: [`arn:aws:s3:::${cerebrumImageBucketName}/*`]
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
      // vpc: args.vpc
    })

    this.handleCerebrumImageTransferRoleArn = handleCerebrumImageTransfer?.role?.roleArn as string

    if (stage === 'prod') {
      /*
       * In 'prod' stage bucket was already there before inception
       * of Charcot, so have to work with what was there already (I.e.
       * unable to drop and recreate it
       */
      const loadedBucket = S3Bucket.fromBucketName(this, 'BucketLoadedByName', cerebrumImageBucketName)
      loadedBucket.addEventNotification(EventType.OBJECT_CREATED, new s3Notifications.LambdaDestination(handleCerebrumImageTransfer))
    } else {
      const cerebrumImageBucket = new Bucket(this, cerebrumImageBucketName, {
        name: cerebrumImageBucketName,
        notifications: {
          myNotification: {
            type: 'function',
            function: handleCerebrumImageTransfer,
            events: ['object_created']
          }
        }
      })
      cerebrumImageBucket.attachPermissions(['s3'])
    }

    // Create a HTTP API
    const hostedZone = route53.HostedZone.fromHostedZoneAttributes(this, 'HostedZone', {
      hostedZoneId: 'Z0341163303ASZWMW1YTS',
      zoneName: 'mountsinaicharcot.org'
    })
    /*
     * Note: W/o explicitly passing in hostedZone, was getting:
     *   'It seems you are configuring custom domains for you URL. And SST is not able to find the hosted zone "mountsinaicharcot.org" in your AWS Route 53 account. Please double check and make sure the zone exists, or pass in a different zone.'
     */
    this.api = new sst.Api(this, 'Api', {
      customDomain: {
        domainName: `${stage === 'prod' ? 'api.mountsinaicharcot.org' : `api-${stage}.mountsinaicharcot.org`}`,
        cdk: {
          hostedZone,
          certificate: Certificate.fromCertificateArn(this, 'MyCert', 'arn:aws:acm:us-east-1:045387143127:certificate/1004f57f-a544-476d-8a31-5b878a71c276')
        }
      },
      routes: {
        'POST /cerebrum-images': {
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
          authorizer: 'iam',
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
    //       be able too hit this endpoint, but not anon ones????? (head scratch)
    //       Experiment,
    //       [REF|https://sst.dev/chapters/adding-auth-to-our-serverless-app.html|"The attachPermissionsForAuthUsers function allows us to specify the resources our authenticated users have access to."]
    auth.attachPermissionsForAuthUsers(this, [this.api])

    this.userPoolId = auth.userPoolId
    this.cognitoIdentityPoolId = auth.cognitoIdentityPoolId
    this.userPoolClientId = auth.userPoolClientId
    this.cerebrumImageOrderTableArn = cerebrumImageOrderTable.tableArn
    this.cerebrumImageMetadataTableArn = cerebrumImageMetaDataTable.tableArn
    this.cerebrumImageOrderQueueArn = cerebrumImageOrderQueue.queueArn
    this.addOutputs({
      ApiEndpoint: this.api.customDomainUrl || this.api.url,
      Region: this.region,
      UserPoolId: this.userPoolId,
      CognitoIdentityPoolId: this.cognitoIdentityPoolId!,
      UserPoolClientId: this.userPoolClientId,
      HandleCerebrumImageTransferRoleArn: this.handleCerebrumImageTransferRoleArn,
      CerebrumImageOrderTableArn: this.cerebrumImageOrderTableArn,
      CerebrumImageMetadataTableArn: this.cerebrumImageMetadataTableArn,
      CerebrumImageOrderQueueArn: this.cerebrumImageOrderQueueArn,
      CerebrumImageOrderQueueUrl: cerebrumImageOrderQueue.queueUrl,
      CerebrumImageOrderQueueName: cerebrumImageOrderQueue.queueName
    })
  }
}

const cognitoUserPool = (scope: Construct) => new Auth(scope, 'Auth', {
  login: ['email'],
  cdk: {
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
