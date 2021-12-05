import * as sst from '@serverless-stack/resources'

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
      primaryIndex: { partitionKey: 'fileName' }
    }
    const cerebrumImageMetaDataTable = new sst.Table(this, process.env.CEREBRUM_IMAGE_METADATA_TABLE_NAME as string, tableProps)

    // Create a HTTP API
    const charcotApi = new sst.Api(this, 'Api', {
      routes: {
        'POST /cerebrum-images': {
          function: {
            functionName: process.env.CREATE_CEREBRUM_IMAGES_FUNCTION_NAME,
            handler: 'src/cerebrum-images-lambda.create',
            environment: {
              CEREBRUM_IMAGE_METADATA_TABLE_NAME: cerebrumImageMetaDataTable.tableName
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
