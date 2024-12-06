#!/usr/bin/env zx

/**
 * TODO: Can enhance the aws-sdk-wrapper cognito client to expose a method to provide the JWT given a user/password
 * TODO: Consider populating DynamoDB image metadata from this script as well, essentially the
 *   'npm run import:data:debug' step. Why? Make this script one stop shop to provision debug environment. Idea
 *   occurred to me on 06/10/2024 while testing fulfillment fix to DynamoDB record size 400KB exceeded error
 */

const NOW = new Date().toLocaleDateString()
const STAGE_APP_CLIENT_ID_CONFIG = {
  debug: '2kclujbsiogn47hr7hlm14hn0n',
  jmquij0106: '538kf5r55ifhcq1nkjv38cf3ql',
  prod: '3109so4n3homnuhjlce1fae27u'
}

const fs = require('fs')
const yargs = require('yargs/yargs')
const {
  Readable,
  Writable
} = require('stream')
var jsonArrayStreams = require('json-array-streams')
const {
  axiosClient,
  cognitoIdentityServiceProviderClient
} = require('@exsoinn/aws-sdk-wrappers')
const path = require('path')
const through2 = require('through2')
const { pipeline } = require('node:stream/promises')

const argv = yargs(process.argv.slice(2))
  .usage('Usage: deploy.mjs <action> [options]')
  .example('post-metadata.mjs -s <stage>', 'Upsert brain slide image metadata into AWS DynamoDB')
  .alias('f', 'file')
  .alias('s', 'stage')
  .alias('u', 'username')
  .alias('p', 'password')
  .describe('f', 'Name of the input file in the `data/` folder that contains the data to import in JSON format')
  .describe('s', 'Stage (i.e. environment) to post the image metadata to')
  .describe('u', 'Username you use to log into Charcot')
  .describe('p', 'Password you use to log into Charcot')
  .demandOption(['f', 's', 'u', 'p'])
  .help('h')
  .alias('h', 'help')
  .argv

const {
  file,
  stage,
  username,
  password
} = argv
const input = path.resolve(__dirname, '../', 'data', file)
console.log(`Reading input data from ${input}, stage is ${stage}`)

const accessCode = await obtainCognitoAccessToken(stage, username, password)

// Stores an array of documents which can be "flushed" at will
const subjectToMultiValueCategories = new Map()
const endpoint = calculateEndpoint(stage)
const flushThreshold = 30

// First creates a look-up Map of subject number to multi-value fields
await pipeline(
  fs.createReadStream(input),
  jsonArrayStreams.parse(),
  sanitizeStream(),
  preparePayloadStream(),
  generateMultiValueCategoriesStream(),
  dummyWriteStream()
).catch(e => {
  console.error('Problem generating lookup map of subject number to multi-value fields', e)
})

// The second pass is to add the multi-value fields of each subject number to each file record
await pipeline(
  fs.createReadStream(input),
  jsonArrayStreams.parse(),
  sanitizeStream(),
  preparePayloadStream(),
  submitStream(accessCode),
  dummyWriteStream()
).catch(e => {
  console.error('Problem uploading metadata of multi-value dimensions', e)
})

/**
 * Construct a function to flush any remaining data left in the passed-in buffer. This function
 * is passed to the Transform stream so that Transform API invokes at the end of the stream to deal with any
 * leftover data in the buffer.
 */
function flush(buffer, accessToken) {
  return async (cb) => {
    if (buffer.length > 0) {
      console.log(`Taking care of flushing ${buffer.length} records left in the buffer...`)
      await sendData(buffer, accessToken)
      console.log(`Flushing done`)
      console.log(`Finished!`)
    }
    cb()
  }
}

function sanitizeStream() {
  const sanitizeTargets = ['RegionName', 'Stain', 'Race', 'Sex', 'Disorder']
  return through2.obj(function (chunk, enc, cb) {
    for (const target of sanitizeTargets) {
      chunk[target] = sanitize(chunk[target] || 'unknown')
    }
    this.push(chunk)
    cb()
  })
}

function preparePayloadStream() {
  return through2.obj(async function (chunk, encoding, callback) {
    this.push({
      fileName: chunk.FileName,
      region: chunk.RegionName,
      stain: chunk.Stain,
      age: chunk.Age,
      race: chunk.Race,
      sex: chunk.Sex,
      diagnosis: chunk.Disorder,
      subjectNumber: chunk.SubNum,
      uploadDate: NOW,
      enabled: isStainDisabled(chunk.Stain) ? 'false' : 'true'
    })
    callback()
  })
}

function generateMultiValueCategoriesStream() {
  return through2.obj(function (chunk, enc, cb) {
    let doc
    const subNum = chunk.subjectNumber
    if (!(doc = subjectToMultiValueCategories.get(subNum))) {
      subjectToMultiValueCategories.set(subNum, {
        stains: new Set([chunk.stain]),
        regions: new Set([chunk.region])
      })
    } else {
      !isStainDisabled(chunk.stain) && doc.stains.add(chunk.stain)
      doc.regions.add(chunk.region)
    }
    this.push(chunk)
    cb()
  })
}

function submitStream(accessToken) {
  let buffer = []
  return through2.obj(async function (chunk, encoding, callback) {
    try {
      const subNum = chunk.subjectNumber
      buffer.push({
        allSubjectRegions: convertToString(subjectToMultiValueCategories.get(subNum).regions),
        allSubjectStains: convertToString(subjectToMultiValueCategories.get(subNum).stains),
        ...chunk
      })
      // If buffer is full, flush it
      if (buffer.length > flushThreshold) {
        await sendData(buffer, accessToken)
        while (buffer.length > 0) {
          // remove all elements from buffer that were just flushed. we do it this way instead of 'buffer = []'
          // so that all external references are preserved, I.e. the flush() function holds a reference
          // to this buffer for flushing the last remaining bit
          buffer.pop()
        }
      }
      callback()
    } catch (e) {
      console.error(`Problem posting ${JSON.stringify(buffer)}`, e)
      callback(e)
    }
  }, flush(buffer, accessToken)).on('finish', () => {
    console.log(`JMQ: submitStream write 'finish' event, buffer is ${JSON.stringify(buffer)}`)
  }).on('end', () => {
    console.log(`JMQ: submitStream read 'end' event, buffer is ${JSON.stringify(buffer)}`)
  })
}

function convertToString(set) {
  return Array.from(set.values()).sort().map(e => e.toLowerCase()).join('||').replace(/\s/g, '')
}

function isStainDisabled(stain) {
  return 'Thioflavin S' === stain
}

/**
 * The last stream of the pipeline could have just been a Writeable, but we wanted to leverage
 * the "flush" callback feature of Transform streams (https://nodejs.org/api/stream.html#transform_flushcallback), so
 * need this Writeable as the book end to avoid this behavior:
 * "Writing data while the stream is not draining is particularly problematic for a Transform, because the Transform
 * streams are paused by default until they are piped or a 'data' or 'readable' event handler is added." found in
 * https://nodejs.org/api/stream.html#implementing-a-writable-stream
 * This Writable is piped into the Transform, and it avoids the behavior above because now the Transform we use to leverage the
 * aforementioned "flush" mechanism, is draining.
 */
function dummyWriteStream() {
  return new Writable({
    objectMode: true,
    write(chunk, enc, cb) {
      cb()
    }
  })
}

function calculateEndpoint(stage) {
  return stage === 'prod' ? 'https://api.mountsinaicharcot.org/cerebrum-images' : `https://api-${stage}.mountsinaicharcot.org/cerebrum-images`
}

function sanitize(str) {
  return str.replace(/\s+$/, '')
}

async function obtainCognitoAccessToken(stage, username, password) {
  const userPoolResponse = await cognitoIdentityServiceProviderClient.listUserPools({ MaxResults: 59 }).promise()

  const userPool = userPoolResponse.UserPools.filter(e => e.Name.startsWith(stage)).sort((a, b) => b.CreationDate.getTime() - a.CreationDate.getTime())[0]
  return (await cognitoIdentityServiceProviderClient.adminInitiateAuth({
    AuthFlow: 'ADMIN_USER_PASSWORD_AUTH',
    ClientId: STAGE_APP_CLIENT_ID_CONFIG[stage],
    UserPoolId: userPool.Id,
    AuthParameters: {
      USERNAME: username,
      PASSWORD: password
    }
  }).promise()).AuthenticationResult.AccessToken
}

async function sendData(buffer, accessToken) {
  await axiosClient.post(endpoint, buffer, {
    headers: {
      Authorization: `Bearer ${accessToken}`
    }
  })
  console.log(`Successfully imported ${buffer.length} records`)
  return Promise.resolve()
}
