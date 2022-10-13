import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client'
import { dynamoDbClient } from '@exsoinn/aws-sdk-wrappers'

export default abstract class Search {
  async handleSearch(params: DocumentClient.QueryInput, callback: (scanOutput: DocumentClient.ScanOutput, items: DocumentClient.ItemList) => void) {
    const limit = params.Limit
    let totalItems = 0
    while (true) {
      const res: DocumentClient.ScanOutput = await dynamoDbClient.scan(params)
      // console.debug(`JMQ: handleSearch() params is ${JSON.stringify(params)}, res is ${JSON.stringify(res)}`)

      const lastEvaluatedKey = res.LastEvaluatedKey
      if ((res.Items && res.Items.length) || params.Select === 'COUNT') {
        const items: DocumentClient.ItemList = res.Items ? res.Items : []
        totalItems += items.length
        // console.log(`JMQ: items is ${JSON.stringify(items)}`)
        callback(res, items)
      }

      /*
       * In case caller specified a limit, check if we haven't
       * met it yet. Remember DynamoDB returns a total of 1MB worth
       * of data, so results can be broken up into multiple ages
       * regardless of how small or big DocumentClient.QueryInput.Limit is.
       * See https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#scan-property,
       * search for " if the processed dataset size exceeds 1 MB before DynamoDB reaches this limit".
       * Also when DocumentClient.QueryInput.Limit is specified, DynamoDB will always
       * return a LastEvaluatedKey
       */
      if (lastEvaluatedKey && (!limit || totalItems < limit)) {
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
