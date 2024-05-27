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

const categories = ['age', 'diagnosis', 'race', 'region', 'sex', 'stain']
let startKey = 1
let dataDictionary = {}
let itemCount = 0
let sumItemSizeInBytes = 0
while (startKey) {
  const res = await dynamoDbClient.scan(params)
  for (const item of res.Items) {
    ++itemCount
    const textEncoder = new TextEncoder()
    sumItemSizeInBytes += textEncoder.encode(JSON.stringify(item)).length
    for (const category of categories) {
      const categoryValue = item[category]
      const isNumber = Number.isInteger(categoryValue)
      let categoryInfo
      let lengthPropName = isNumber ? 'maxValueFoundInDB' : 'maxValueLengthFoundInDB'
      if (!(categoryInfo = dataDictionary[category])) {
        categoryInfo = {
          dataType: isNumber ? 'numeric' : 'string',
          uniqueValues: new Set(),
          [lengthPropName]: isNumber ? categoryValue : categoryValue.length
        }
        dataDictionary[category] = categoryInfo
      }

      // We expect either strings or numbers, so calculate max accordingly
      if (isNumber && categoryValue > categoryInfo[lengthPropName]) {
        categoryInfo[lengthPropName] = categoryValue
      } else if (categoryValue.length > categoryInfo[lengthPropName]) {
        categoryInfo[lengthPropName] = categoryValue.length
      }
      categoryInfo.uniqueValues.add(categoryValue)
    }
  }
  startKey = params.ExclusiveStartKey = res.LastEvaluatedKey
}

dataDictionary = {
  generalInfo: {
    dynamoDbRecordSizeLimitInBytes: 400000,
    averageCharcotImageMetadataRecordSizeInBytes: Math.trunc(sumItemSizeInBytes / itemCount)
  },
  ...dataDictionary
}
printDataDictionary(dataDictionary, categories)

function printDataDictionary(dataDictionary, categories) {
  for (const category of categories) {
    dataDictionary[category].lengthLimit = 'none'
    dataDictionary[category].uniqueValues = Array.from(dataDictionary[category].uniqueValues).sort()
    dataDictionary[category].averageValueLength = Math.trunc(dataDictionary[category].uniqueValues.reduce((sum, val) => sum + (val.length || val), 0) / dataDictionary[category].uniqueValues.length)
  }
  console.log(JSON.stringify(dataDictionary))
}
