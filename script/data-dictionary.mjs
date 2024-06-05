#!/usr/bin/env zx

/**
 * Use this script for a read out of unique values (aka categories in Charcot vernacular) for each category
 * (aka dimension in Charcot vernacular). Harry requested this for first time on 06/05/2024 for David Gutman
 *
 */

const { dynamoDbClient } = require('@exsoinn/aws-sdk-wrappers')

const params = {
  TableName: 'prod-charcot-cerebrum-image-metadata',
  ExpressionAttributeNames: {
    '#subjectNumber': 'subjectNumber',
    '#region': 'region',
    '#stain': 'stain',
    '#sex': 'sex',
    '#race': 'race',
    '#age': 'age',
    '#diagnosis': 'diagnosis',
    '#fileName': 'fileName'
  },
  ProjectionExpression: '#subjectNumber, #region, #stain, #sex, #race, #age, #fileName, #diagnosis'
}

const categories = ['age', 'diagnosis', 'race', 'region', 'sex', 'stain', ]
let startKey = 1
const data = {}
while (startKey) {
  const res = await dynamoDbClient.scan(params)
  for (const item of res.Items) {
    for (const category of categories) {
      let values
      if (!(values = data[category])) {
        values = new Set()
        data[category] = values
      }
      values.add(item[category])
    }
  }
  startKey = params.ExclusiveStartKey = res.LastEvaluatedKey
}

printData(data, categories)

function printData(data, categories) {
  for (const category of categories) {
    console.log(`#################### ${category} ####################`)
    console.log(Array.from(data[category]).sort().join('\n'))
  }
}
