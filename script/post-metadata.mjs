#!/usr/bin/env zx

const fs = require('fs')
const { Writable } = require('stream')
var jsonArrayStreams = require('json-array-streams')
const { axiosClient } = require('@exsoinn/aws-sdk-wrappers')

const stream = fs.createReadStream('/Users/jmquij0106/Downloads/CharcotData.json')

const sanitize = (str) => {
  return str.replace(/\s\(\d+\)/, '')
}
let buffer = []
const flushThreshold = 10
stream.pipe(jsonArrayStreams.parse())
  .pipe(new Writable({
    objectMode: true,
    async write (chunk, encoding, callback) {
      let body
      try {
        buffer.push({
          fileName: chunk.FileName,
          region: chunk.BrainRegion || 'unknown',
          stain: chunk.Stain,
          age: chunk.b[0].Age,
          race: sanitize(chunk.b[0].c[0].Race || 'unknown'),
          sex: sanitize(chunk.b[0].c[0].d[0].Sex),
          uploadDate: '04/08/2022'
        })
        if (buffer.length > flushThreshold) {
          // Toggle between demo AWS and Mt Sinai's accounts. Top is "demo", bottom is Mt Sinai
          // await axiosClient.post('https://5oiylsl5xk.execute-api.us-east-1.amazonaws.com/cerebrum-images', buffer)
          await axiosClient.post('https://5oiylsl5xk.execute-api.us-east-1.amazonaws.com/cerebrum-images', buffer)
          console.log(`JMQ: Successfully posted ${JSON.stringify(buffer)}`)
          buffer = []
        }
        callback()
      } catch (e) {
        console.error(`Problem posting ${JSON.stringify(body)}`, e)
        callback(e)
      }
    }
  }))
