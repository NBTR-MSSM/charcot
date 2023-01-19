#!/usr/bin/env zx

const fs = require('fs')
const yargs = require('yargs/yargs')
const { Writable } = require('stream')
var jsonArrayStreams = require('json-array-streams')
const { axiosClient } = require('@exsoinn/aws-sdk-wrappers')
const path = require('path')

const argv = yargs(process.argv.slice(2))
  .usage('Usage: deploy.mjs <action> [options]')
  .example('post-metadata.mjs -s <stage>', 'Upsert brain slide image metadata into AWS DynamoDB')
  .alias('s', 'stage')
  .describe('s', 'Stage (i.e. environment) to post the image metadata to')
  .demandOption(['s'])
  .help('h')
  .alias('h', 'help')
  .argv

const { stage } = argv
const input = path.resolve(__dirname, '../', 'data', 'charcot-meta-data-20220603.json')
console.log(`Reading input data from ${input}`)
const stream = fs.createReadStream(input)

console.log(path.resolve(__dirname, 'data'))

// Stores an array of documents which can be "flushed" at will
let buffer = []
const endpoint = calculateEndpoint(stage)
const flushThreshold = 30
stream.pipe(jsonArrayStreams.parse())
  .pipe(new Writable({
    objectMode: true,
    async write(chunk, encoding, callback) {
      let body
      console.log(`JMQ: processing chunk ${JSON.stringify(chunk)}`)
      try {
        buffer.push({
          fileName: chunk.FileName,
          region: sanitize(chunk.RegionName || 'unknown'),
          stain: sanitize(chunk.Stain || 'unknown'),
          age: chunk.Age,
          race: sanitize(chunk.Race || 'unknown'),
          sex: sanitize(chunk.Sex),
          diagnosis: sanitize(chunk.Disorder),
          subjectNumber: chunk.SubNum,
          uploadDate: '06/03/2022',
          enabled: 'Thioflavin S' === chunk.Stain ? 'false' : 'true'
        })
        // If buffer is full, flush it
        if (buffer.length > flushThreshold) {
          await sendData(buffer)
          buffer = []
        }
        callback()
      } catch (e) {
        console.error(`Problem posting ${JSON.stringify(body)}`, e)
        callback(e)
      }
    }
  })).on('finish', async () => {
  // send any leftover data (I.e. data ran out and flush threshold was not met)
  if (buffer) {
    console.log(`Taking care of ${buffer.length} records left in the buffer...`)
    await sendData(buffer)
    console.log(`Finished!`)
  }
})

function calculateEndpoint(stage) {
  return stage === 'prod' ? 'https://api.mountsinaicharcot.org/cerebrum-images' : `https://api-${stage}.mountsinaicharcot.org/cerebrum-images`
}

function sanitize(str) {
  console.log(`JMQ: sanitize ${str}`)
  return str.replace(/\s+$/, '')
}

async function sendData(buffer) {
  await axiosClient.post(endpoint, buffer) // DEBUG
  console.log(`JMQ: Successfully posted ${JSON.stringify(buffer)}`)
}
