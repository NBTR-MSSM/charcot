#!/usr/bin/env zx

/**
 * This script loads a lookup using fileName as key, and we iterate over every metadata record in Charcot
 * to see where there are discrepancies in stain, region and diagnosis. This arose from issue found in WH data
 * while testing AND'ing of search criterion across subNum's
 *
 */

const { dynamoDbClient } = require('@exsoinn/aws-sdk-wrappers')
const fs = require('fs')
const isEqual = require('lodash.isequal')
const path = require('path')

// TODO: Trim whitespace, get rid of double quotes in input data. Do that in the 'awk' parse script
// TODO: Load dataWarehouseLookupTable into hash table keyed by fileName for O(1) lookup
const dataWarehouseLookupTable = loadDataWarehouseLookupTable()
const diagnosisLookupTable = loadDiagnosisLookupTable()

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

let startKey = 1
let diffCount = 0
const diffReport = []
while (startKey) {
  const res = await dynamoDbClient.scan(params)
  for (const charcotItem of res.Items) {
    const dataWarehouseItem = dataWarehouseLookupTable[charcotItem.fileName]
    if (dataWarehouseItem) {
      if (dataWarehouseItem.race === 'NULL') {
        dataWarehouseItem.race = 'unknown'
      }

      dataWarehouseItem.diagnosis = diagnosisLookupTable[charcotItem.subjectNumber]

      // JMQ: testing rule out false positives
      //dataWarehouseItem.diagnosis = charcotItem.diagnosis
      //dataWarehouseItem.region = charcotItem.region
      //dataWarehouseItem.stain = charcotItem.stain
      //dataWarehouseItem.sex = charcotItem.sex
      //dataWarehouseItem.race = charcotItem.race
    }
    if (dataWarehouseItem && !isEqual(dataWarehouseItem, charcotItem)) {
      ++diffCount
      diffReport.push([charcotItem, dataWarehouseItem])
      //console.log(`Found diffs:\nDW item is      ${JSON.stringify(sorted(dataWarehouseItem))}\nCharcot item is ${JSON.stringify(sorted(charcotItem))}\n\n`)
    }
  }
  startKey = params.ExclusiveStartKey = res.LastEvaluatedKey
}

console.log(`Diff count: ${diffCount}`)
writeDiffReport(diffReport)

function sorted(obj) {
  const keys = Object.keys(obj).sort()
  const sortedByKey = {}
  keys.map(k => {
    sortedByKey[k] = obj[k]
  })
  return sortedByKey
}

function loadDataWarehouseLookupTable() {
  return JSON.parse(fs.readFileSync(`${process.env.HOME}/Library/Application Support/JetBrains/WebStorm2023.3/scratches/scratch-charcot-brain-slide-image-metadata-from-oleg-20240512.json`))
}

/**
 * Returns map of subject number and diagnosis
 */
function loadDiagnosisLookupTable() {
  return JSON.parse(fs.readFileSync(`${process.env.HOME}/Library/Application Support/JetBrains/WebStorm2023.3/scratches/scratch-charcot-diagnosis-lookup-table-from-harry-20240515.json`))
}

function writeDiffReport(diffReport) {
  const sep = '\t'
  const csvLines = [`source${sep}subjectNumber${sep}region${sep}stain${sep}sex${sep}race${sep}age${sep}diagnosis${sep}fileName`]
  const jsonArray = []
  for (const tuple of diffReport) {
    const {
      subjectNumber,
      region,
      stain,
      sex,
      race,
      age,
      fileName,
      diagnosis
    } = tuple[0]
    jsonArray.push({
      FileName: fileName,
      SubNum: subjectNumber,
      Age: age,
      Sex: sex,
      Race: race,
      Disorder: diagnosis,
      Stain: stain,
      RegionName: region
    })
    let source = 'charcot'
    for (const obj of tuple) {
      const {
        subjectNumber,
        region,
        stain,
        sex,
        race,
        age,
        diagnosis,
        fileName
      } = obj
      csvLines.push(`${source}${sep}${subjectNumber}${sep}${region}${sep}${stain}${sep}${sex}${sep}${race}${sep}${age}${sep}${diagnosis}${sep}${fileName}`)
      source = 'dw'
    }
  }
  fs.writeFileSync(path.join(__dirname, '../', 'data', 'charcot-meta-data-region-stain-diagnosis-correction-20240629.json'), JSON.stringify(jsonArray))
  fs.writeFileSync(path.join(require('os').homedir(), 'Downloads', 'charcot-meta-data-region-stain-diagnosis-correction-diff-report-20240629.tsv'), csvLines.join('\n'))
}

