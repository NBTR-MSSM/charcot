import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client'
import { dynamoDbClient } from '@exsoinn/aws-sdk-wrappers'

export default abstract class Search {
  async handleSearch(params: DocumentClient.QueryInput, callback: (items: DocumentClient.ItemList) => void) {
    while (true) {
      const res: DocumentClient.ScanOutput = await dynamoDbClient.scan(params)
      console.debug(`JMQ: handleSearch() params is ${JSON.stringify(params)}, res.Items is ${JSON.stringify(res)}`)

      const lastEvaluatedKey = res.LastEvaluatedKey
      if (res.Items && res.Items.length) {
        const items: DocumentClient.ItemList = res.Items
        console.log(`JMQ: items is ${JSON.stringify(items)}`)
        callback(items)
      }

      if (lastEvaluatedKey) {
        params = {
          ...params,
          ExclusiveStartKey: lastEvaluatedKey
        }
      } else {
        break
      }
    }
  }
}
