import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client'
import { dynamoDbClient } from '@exsoinn/aws-sdk-wrappers'

export default abstract class Search {
  async handleSearch(params: DocumentClient.QueryInput, callback: (scanOutput: DocumentClient.ScanOutput, items: DocumentClient.ItemList) => void) {
    while (true) {
      const res: DocumentClient.ScanOutput = await dynamoDbClient.scan(params)
      const lastEvaluatedKey = res.LastEvaluatedKey
      if (res.Items && res.Items.length) {
        const items: DocumentClient.ItemList = res.Items
        callback(res, items)
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
