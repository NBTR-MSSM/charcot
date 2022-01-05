import * as sst from '@serverless-stack/resources'
import { Bucket } from '@serverless-stack/resources'
import * as iam from '@aws-cdk/aws-iam'

export default class CharcotStack extends sst.Stack {
  constructor(scope: sst.App, id: string, props?: sst.StackProps) {
    super(scope, id, props)
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
      primaryIndex: { partitionKey: 'fileName' }
    }

    const
      cerebrumImageOrderTable = new sst.Table(this, process.env.CEREBRUM_IMAGE_ORDER_TABLE_NAME as string, cerebrumImageOrderTableProps)

    const
      cerebrumImageBucket = new Bucket(this, process.env.CEREBRUM_IMAGE_BUCKET_NAME as string, {
        s3Bucket: {
          bucketName: process.env.CEREBRUM_IMAGE_BUCKET_NAME
        }
      })
    const
      cerebrumImageZipBucket = new Bucket(this, process.env.CEREBRUM_IMAGE_ZIP_BUCKET_NAME as string, {
        s3Bucket: {
          bucketName: process.env.CEREBRUM_IMAGE_ZIP_BUCKET_NAME as string
        }
      })

    const
      handleCerebrumImageFulfillment = new sst.Function(this, 'HandleCerebrumImageFulfillment', {
        functionName: process.env.HANDLE_CEREBRUM_IMAGE_FULFILLMENT_FUNCTION_NAME,
        handler: 'src/lambda/cerebrum-image-fulfillment.handle',
        initialPolicy: [
          new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            actions: ['dynamodb:GetItem'],
            resources: [cerebrumImageMetaDataTable.tableArn]
          }),
          new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            actions: ['s3:GetObject'],
            resources: [`${cerebrumImageBucket.bucketArn}/*`]
          }),
          new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            actions: ['s3:PutObject'],
            resources: [`${cerebrumImageZipBucket.bucketArn}/*`]
          }),
          new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            actions: ['ses:SendEmail'],
            resources: ['*']
          })],
        environment: {
          CEREBRUM_IMAGE_BUCKET_NAME: cerebrumImageBucket.bucketName,
          CEREBRUM_IMAGE_ZIP_BUCKET_NAME: cerebrumImageZipBucket.bucketName,
          CEREBRUM_IMAGE_ORDER_TABLE_NAME: cerebrumImageOrderTable.tableName
        },
        timeout: 900
      })

    // Create a HTTP API
    const
      charcotApi = new sst.Api(this, 'Api', {
        routes: {
          'POST /cerebrum-images': {
            function: {
              functionName: process.env.CREATE_CEREBRUM_IMAGE_METADATA_FUNCTION_NAME,
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
              functionName: process.env.HANDLE_CEREBRUM_IMAGE_SEARCH_FUNCTION_NAME,
              handler: 'src/lambda/cerebrum-image-search.handle',
              initialPolicy: [
                new iam.PolicyStatement({
                  effect: iam.Effect.ALLOW,
                  actions: ['dynamodb:Query'],
                  resources: [cerebrumImageMetaDataTable.tableArn, `${cerebrumImageMetaDataTable.tableArn}/index/*`]
                })],
              environment: {
                CEREBRUM_IMAGE_METADATA_TABLE_NAME: cerebrumImageMetaDataTable.tableName,
                HANDLE_CEREBRUM_IMAGE_FULFILLMENT_FUNCTION_NAME: process.env.HANDLE_CEREBRUM_IMAGE_FULFILLMENT_FUNCTION_NAME as string
              }
            }
          },
          'POST /cerebrum-image-orders': {
            function: {
              functionName: process.env.CREATE_CEREBRUM_IMAGE_ORDER_FUNCTION_NAME,
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
                CEREBRUM_IMAGE_ORDER_TABLE_NAME: cerebrumImageOrderTable.tableName
              }
            }
          }
        }
      })

    charcotApi
      .attachPermissions([cerebrumImageMetaDataTable])

    // Show the endpoint in the output
    this
      .addOutputs({
        ApiEndpoint: charcotApi.url
      })
  }
}
