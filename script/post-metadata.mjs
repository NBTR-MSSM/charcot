#!/usr/bin/env zx

const fs = require('fs')
const { Writable } = require('stream')
var jsonArrayStreams = require('json-array-streams')
const { axiosClient } = require('@exsoinn/aws-sdk-wrappers')
const path = require('path')

const input = path.resolve(__dirname, '../', 'data', 'charcot-meta-data-20220603.json')
// const input = path.resolve(__dirname, '../', 'data', 'test.json')
console.log(`Reading input data from ${input}`)
const stream = fs.createReadStream(input)

console.log(path.resolve(__dirname, 'data'))

const sanitize = (str) => {
  console.log(`JMQ: sanitize ${str}`)
  return str.replace(/\s+$/, '')
}

const sendData = async (buffer) => {
  await axiosClient.post('https://api-debug.mountsinaicharcot.org/cerebrum-images', buffer) // LOCAL
  // await axiosClient.post('https://api-debug.mountsinaicharcot.org/cerebrum-images', buffer) // DEBUG
  //await axiosClient.post('https://api-dev.mountsinaicharcot.org/cerebrum-images', buffer) // DEV
  // await axiosClient.post('https://wq2rjam09d.execute-api.us-east-1.amazonaws.com/cerebrum-images', buffer) // PROD
  console.log(`JMQ: Successfully posted ${JSON.stringify(buffer)}`)
}

// Stores an array of documents which can be "flushed" at will
let buffer = []

const flushThreshold = 30
stream.pipe(jsonArrayStreams.parse())
  .pipe(new Writable({
    objectMode: true,
    async write (chunk, encoding, callback) {
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
  // send any left over data (I.e. data ran out and flush threshold was not met)
  if (buffer) {
    console.log(`Taking care of ${buffer.length} records left in the buffer...`)
    await sendData(buffer)
    console.log(`Finished!`)
  }
})
