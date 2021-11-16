import * as sst from '@serverless-stack/resources'
import { Api, Table } from '@serverless-stack/resources'

export default class CharcotStack extends sst.Stack {
  // FIXME: These don't need to be instance members
  cerebrumImageMetaDataTable: Table
  charcotApi: Api

  constructor(scope: sst.App, id: string, props?: sst.StackProps) {
    super(scope, id, props)
    this.cerebrumImageMetaDataTable = new sst.Table(this, process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string, {
      fields: {
        fileName: sst.TableFieldType.STRING,
        regionName: sst.TableFieldType.STRING,
        stain: sst.TableFieldType.STRING,
        age: sst.TableFieldType.NUMBER,
        race: sst.TableFieldType.STRING,
        sex: sst.TableFieldType.STRING,
        uploadDate: sst.TableFieldType.STRING
      },
      primaryIndex: { partitionKey: 'fileName' }
    })

    // Create a HTTP API
    this.charcotApi = new sst.Api(this, 'Api', {
      routes: {
        'POST /cerebrum-images': {
          function: {
            functionName: process.env.CREATE_CEREBRUM_IMAGES_FUNCTION_NAME,
            handler: 'src/cerebrum-images-lambda.create',
            environment: {
              TABLE_NAME: this.cerebrumImageMetaDataTable.tableName
            }
          }
        }
      }
    })

    this.charcotApi.attachPermissions([this.cerebrumImageMetaDataTable])

    // Show the endpoint in the output
    this.addOutputs({
      ApiEndpoint: this.charcotApi.url
    })
  }
}
