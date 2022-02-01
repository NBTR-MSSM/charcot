import * as sst from '@serverless-stack/resources'
import { Bucket } from '@serverless-stack/resources'
import * as iam from 'aws-cdk-lib/aws-iam'
import { Bucket as S3Bucket, EventType } from 'aws-cdk-lib/aws-s3'
import * as s3Notifications from 'aws-cdk-lib/aws-s3-notifications'

export default class CharcotStack extends sst.Stack {
  constructor(scope: sst.App, id: string, props?: sst.StackProps) {
    super(scope, id, props)

    // DynamoDB Tables
    const cerebrumImageMetaDataTableProps = {
      fields: {
        fileName: sst.TableFieldType.STRING,
        regionName: sst.TableFieldType.STRING,
        stain: sst.TableFieldType.STRING,
        age: sst.TableFieldType.NUMBER,
        race: sst.TableFieldType.STRING,
        sex: sst.TableFieldType.STRING,
        uploadDate: sst.TableFieldType.STRING
      },
      primaryIndex: { partitionKey: 'fileName' },
      globalIndexes: {
        regionNameIndex: { partitionKey: 'regionName' },
        stainIndex: { partitionKey: 'stain' },
        ageIndex: { partitionKey: 'age' },
        raceIndex: { partitionKey: 'race' },
        sexIndex: { partitionKey: 'sex', sortKey: 'fileName' }
      }
    }
    const cerebrumImageMetaDataTable = new sst.Table(this, process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string, cerebrumImageMetaDataTableProps)

    const cerebrumImageOrderTableProps = {
      fields: {
        orderId: sst.TableFieldType.STRING,
        email: sst.TableFieldType.STRING,
        created: sst.TableFieldType.STRING
      },
      primaryIndex: { partitionKey: 'orderId' }
    }

    const cerebrumImageOrderTable = new sst.Table(this, process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME as string, cerebrumImageOrderTableProps)

    const stage = this.stage

    const handleCerebrumImageTransferFunctionName = `${process.env.HANDLE_CEREBRUM_IMAGE_TRANSFER_FUNCTION_NAME}-${stage}`
    const handleCerebrumImageFulfillmentFunctionName = `${process.env.HANDLE_CEREBRUM_IMAGE_FULFILLMENT_FUNCTION_NAME}-${stage}`
    const createCerebrumImageMetadataFunctionName = `${process.env.CREATE_CEREBRUM_IMAGE_METADATA_FUNCTION_NAME}-${stage}`
    const handleCerebrumImageSearchFunctionName = `${process.env.HANDLE_CEREBRUM_IMAGE_SEARCH_FUNCTION_NAME}-${stage}`
    const createCerebrumImageOrderFunctionName = `${process.env.CREATE_CEREBRUM_IMAGE_ORDER_FUNCTION_NAME}-${stage}`

    // Mt Sinai had no concept of stages prior to Charcot, so need the below for backward compatibility
    // with their stage-less S3 buckets which were in place already before Charcot. Renaming
    // those existing buckets is not an option
    const bucketStage = stage === 'prod' ? '' : `-${stage}`
    const cerebrumImageBucketName = `${process.env.CEREBRUM_IMAGE_BUCKET_NAME}${bucketStage}`
    const cerebrumImageOdpBucketName = `${process.env.CEREBRUM_IMAGE_ODP_BUCKET_NAME}${bucketStage}`
    const cerebrumImageZipBucketName = `${process.env.CEREBRUM_IMAGE_ZIP_BUCKET_NAME}${bucketStage}`

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
    })

    if (stage === 'prod') {
      const loadedBucket = S3Bucket.fromBucketName(this, 'BucketLoadedByName', cerebrumImageBucketName)
      loadedBucket.addEventNotification(EventType.OBJECT_CREATED, new s3Notifications.LambdaDestination(handleCerebrumImageTransfer))
    } else {
      const cerebrumImageBucket = new Bucket(this, cerebrumImageBucketName, {
        s3Bucket: {
          bucketName: cerebrumImageBucketName
        },
        notifications: [
          {
            function: handleCerebrumImageTransfer,
            notificationProps: {
              events: [EventType.OBJECT_CREATED]
            }
          }
        ]
      })
      cerebrumImageBucket.attachPermissions(['s3'])
    }

    // Functions
    // TODO: See if Lambda memory size can be reduced by inspecting logs to see exactly how much memory used
    //  Also might need to asynchronously invoke multiple times the Lambda to create smaller, Zips so that each running
    //  instance stays below the memory limit
    const handleCerebrumImageFulfillment = new sst.Function(this, 'HandleCerebrumImageFulfillment', {
      functionName: handleCerebrumImageFulfillmentFunctionName,
      handler: 'src/lambda/cerebrum-image-fulfillment.handle',
      memorySize: 10240,
      initialPolicy: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: ['dynamodb:GetItem'],
          resources: [cerebrumImageOrderTable.tableArn]
        }),
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: ['s3:GetObject'],
          resources: [`arn:aws:s3:::${cerebrumImageOdpBucketName}/*`]
        }),
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: ['s3:ListBucket'],
          resources: [`arn:aws:s3:::${cerebrumImageOdpBucketName}`]
        }),
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: ['s3:PutObject', 's3:GetObject'],
          resources: [`arn:aws:s3:::${cerebrumImageZipBucketName}/*`]
        }),
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: ['ses:SendEmail'],
          resources: ['*']
        })],
      environment: {
        CEREBRUM_IMAGE_ODP_BUCKET_NAME: cerebrumImageOdpBucketName,
        CEREBRUM_IMAGE_ZIP_BUCKET_NAME: cerebrumImageZipBucketName,
        CEREBRUM_IMAGE_ORDER_TABLE_NAME: cerebrumImageOrderTable.tableName,
        FROM_EMAIL: process.env.FROM_EMAIL as string
      },
      timeout: 900
    })

    // Create a HTTP API
    const
      charcotApi = new sst.Api(this, 'Api', {
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
              handler: 'src/lambda/cerebrum-image-search.handle',
              initialPolicy: [
                new iam.PolicyStatement({
                  effect: iam.Effect.ALLOW,
                  actions: ['dynamodb:Query'],
                  resources: [cerebrumImageMetaDataTable.tableArn, `${cerebrumImageMetaDataTable.tableArn}/index/*`]
                })],
              environment: {
                CEREBRUM_IMAGE_METADATA_TABLE_NAME: cerebrumImageMetaDataTable.tableName
              }
            }
          },
          'POST /cerebrum-image-orders': {
            function: {
              functionName: createCerebrumImageOrderFunctionName,
              handler: 'src/lambda/cerebrum-image-order.create',
              initialPolicy: [
                new iam.PolicyStatement({
                  effect: iam.Effect.ALLOW,
                  actions: ['lambda:InvokeAsync', 'lambda:InvokeFunction'],
                  resources: [handleCerebrumImageFulfillment.functionArn]
                }),
                new iam.PolicyStatement({
                  effect: iam.Effect.ALLOW,
                  actions: ['dynamodb:PutItem'],
                  resources: [cerebrumImageOrderTable.tableArn]
                })],
              environment: {
                CEREBRUM_IMAGE_ORDER_TABLE_NAME: cerebrumImageOrderTable.tableName,
                HANDLE_CEREBRUM_IMAGE_FULFILLMENT_FUNCTION_NAME: handleCerebrumImageFulfillmentFunctionName
              }
            }
          }
        }
      })

    charcotApi.attachPermissions([cerebrumImageMetaDataTable])

    // Show the endpoint in the output. Have to "escape" underscores this way
    // because SST eats anything that's not alphabetic
    this.addOutputs({
      ApiEndpoint: charcotApi.url,
      HANDLExUNDERxCEREBRUMxUNDERxIMAGExUNDERxFULFILLMENTxUNDERxROLExUNDERxARN: handleCerebrumImageFulfillment.role?.roleArn as string,
      HANDLExUNDERxCEREBRUMxUNDERxIMAGExUNDERxTRANSFERxUNDERxROLExUNDERxARN: handleCerebrumImageTransfer.role?.roleArn as string
    })
  }
}
