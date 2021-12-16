import * as sst from '@serverless-stack/resources'
import * as iam from '@aws-cdk/aws-iam'
import { Bucket } from '@serverless-stack/resources'

export default class CharcotStack extends sst.Stack {
  constructor(scope: sst.App, id: string, props?: sst.StackProps) {
    super(scope, id, props)
    const tableProps = {
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

    const cerebrumImageBucket = new Bucket(this, process.env.CEREBRUM_IMAGE_BUCKET_NAME as string, {
      s3Bucket: {
        bucketName: process.env.CEREBRUM_IMAGE_BUCKET_NAME
      }
    })
    const cerebrumImageZipBucket = new Bucket(this, process.env.CEREBRUM_IMAGE_ZIP_BUCKET_NAME as string, {
      s3Bucket: {
        bucketName: process.env.CEREBRUM_IMAGE_ZIP_BUCKET_NAME as string
      }
    })

    const cerebrumImageMetaDataTable = new sst.Table(this, process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string, tableProps)

    const handleCerebrumImageRequest = new sst.Function(this, 'HandleCerebrumImageRequest', {
      functionName: process.env.HANDLE_CEREBRUM_IMAGE_REQUEST_FUNCTION_NAME,
      handler: 'src/lambda/cerebrum-image-request-processor.handle',
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
        })],
      environment: {
        CEREBRUM_IMAGE_BUCKET_NAME: cerebrumImageBucket.bucketName,
        CEREBRUM_IMAGE_ZIP_BUCKET_NAME: cerebrumImageZipBucket.bucketName
      },
      timeout: 900
    })

    // Create a HTTP API
    const charcotApi = new sst.Api(this, 'Api', {
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
            functionName: process.env.HANDLE_CEREBRUM_IMAGE_SELECTION_EXPERIENCE_FUNCTION_NAME,
            handler: 'src/lambda/cerebrum-image-selection-experience.handle',
            initialPolicy: [
              new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: ['lambda:InvokeAsync', 'lambda:InvokeFunction'],
                resources: [handleCerebrumImageRequest.functionArn]
              }),
              new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: ['dynamodb:Query'],
                resources: [cerebrumImageMetaDataTable.tableArn, `${cerebrumImageMetaDataTable.tableArn}/index/*`]
              })],
            environment: {
              CEREBRUM_IMAGE_METADATA_TABLE_NAME: cerebrumImageMetaDataTable.tableName,
              HANDLE_CEREBRUM_IMAGE_REQUEST_FUNCTION_NAME: process.env.HANDLE_CEREBRUM_IMAGE_REQUEST_FUNCTION_NAME as string
            }
          }
        }
      }
    })

    charcotApi.attachPermissions([cerebrumImageMetaDataTable])

    // Show the endpoint in the output
    this.addOutputs({
      ApiEndpoint: charcotApi.url
    })
  }
}
