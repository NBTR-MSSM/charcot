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

    /*
     * SQS Queue(s)
     * The delivery delay is set to give ECS ample time to scale out so that workers are already started before
     * the first message is received. W/o the delay, yes scale out will take place, but by that time the one
     * worker will end up during most of the work. Thus the delivery delay results in even distribution among
     * the existing and the scale out newly added workers
     */
    const cerebrumImageOrderQueue = new sst.Queue(this, process.env.CEREBRUM_IMAGE_ORDER_QUEUE_NAME as string, {
      cdk: {
        queue: {
          deliveryDelay: Duration.minutes(5),
          visibilityTimeout: Duration.hours(12),
          receiveMessageWaitTime: Duration.seconds(20)
        }
      }
    })

    // DynamoDB Tables
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
        created: 'string',
        filter: 'string'
      },
      primaryIndex: { partitionKey: 'orderId' }
    })

    const stage = this.stage

    const handleCerebrumImageTransferFunctionName = `${process.env.HANDLE_CEREBRUM_IMAGE_TRANSFER_FUNCTION_NAME}-${stage}`
    const createCerebrumImageMetadataFunctionName = `${process.env.CREATE_CEREBRUM_IMAGE_METADATA_FUNCTION_NAME}-${stage}`
    const handleCerebrumImageSearchFunctionName = `${process.env.HANDLE_CEREBRUM_IMAGE_SEARCH_FUNCTION_NAME}-${stage}`
    const handleCerebrumImageDimensionFunctionName = `${process.env.HANDLE_CEREBRUM_IMAGE_DIMENSION_FUNCTION_NAME}-${stage}`
    const createCerebrumImageOrderFunctionName = `${process.env.CREATE_CEREBRUM_IMAGE_ORDER_FUNCTION_NAME}-${stage}`

    // Mt Sinai had no concept of stages prior to Charcot, so need the below for backward compatibility
    // with their stage-less S3 buckets which were in place already before Charcot. Renaming
    // those existing buckets is not an option
    const bucketSuffix = stage === 'prod' ? '' : `-${stage}`
    const cerebrumImageBucketName = `${process.env.CEREBRUM_IMAGE_BUCKET_NAME}${bucketSuffix}` // source s3 bucket
    const cerebrumImageOdpBucketName = `${process.env.CEREBRUM_IMAGE_ODP_BUCKET_NAME}${bucketSuffix}` // target s3 bucket

    // Buckets and notification target functions
    const handleCerebrumImageTransfer = new sst.Function(this, 'HandleCerebrumImageTransfer', {
      functionName: handleCerebrumImageTransferFunctionName,
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
      defaults: {
        /* function: {
          vpc: args.vpc
        } */
      },
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
            functionName: createCerebrumImageMetadataFunctionName,
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
            functionName: handleCerebrumImageSearchFunctionName,
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
            functionName: handleCerebrumImageDimensionFunctionName,
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
        'POST /cerebrum-image-orders': {
          authorizer: 'iam',
          function: {
            functionName: createCerebrumImageOrderFunctionName,
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
              CEREBRUM_IMAGE_METADATA_TABLE_NAME: cerebrumImageMetaDataTable.tableName,
              FULFILLMENT_HOST: process.env.FULFILLMENT_HOST as string
            }
          }
        }
      }
    })

    // Auth
    const auth = new Auth(this, 'Auth', {
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
