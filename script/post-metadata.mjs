#!/usr/bin/env zx

/**
 * TODO: Automatically obtain bearer token from cognito using user/password. Add YARG arguments to collect
 *   these, then use Cognito SDK to obtain this token and pass it in the Axios POSTS
 *   - Can enhance the aws-sdk-wrapper cognito client to expose a method to provide the JWT given a user/password
 * TODO: Consider populating DynamoDB image metadata from this script as well, essentially the
 *   'npm run import:data:debug' step. Why? Make this script one stop shop to provision debug environment. Idea
 *   occurred to me on 06/10/2024 while testing fulfillment fix to DynamoDB record size 400KB exceeded error
 */

const fs = require('fs')
const yargs = require('yargs/yargs')
const { Readable, Writable } = require('stream')
var jsonArrayStreams = require('json-array-streams')
const { axiosClient } = require('@exsoinn/aws-sdk-wrappers')
const path = require('path')
const through2 = require('through2')
const { pipeline } = require('node:stream/promises')

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
const input = path.resolve(__dirname, '../', 'data', 'charcot-meta-data-region-stain-diagnosis-correction-20240629.json')
//const input = path.resolve(__dirname, '../', 'data', 'charcot-meta-data-20220603.json')
//const input = path.resolve(__dirname, '../', 'data', 'test.json')
console.log(`Reading input data from ${input}`)
console.log(path.resolve(__dirname, 'data'))

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
  console.error('Problem uploading metadata', e)
})

// The second pass is to add the multi-value fields of each subject number to each file record
await pipeline(
  fs.createReadStream(input),
  jsonArrayStreams.parse(),
  sanitizeStream(),
  preparePayloadStream(),
  submitStream(),
  dummyWriteStream()
).catch(e => {
  console.error('Problem uploading metadata of multi-value dimensions', e)
})

/**
 * Construct a function to flush any remaining data left in the passed-in buffer. This function
 * is passed to the Transform stream so that Transform API invokes at the end of the stream to deal with any
 * leftover data in the buffer.
 */
function flush(buffer) {
  return async (cb) => {
    if (buffer.length > 0) {
      console.log(`Taking care of flushing ${buffer.length} records left in the buffer...`)
      await sendData(buffer)
      console.log(`JMQ: flushed ${JSON.stringify(buffer)}`)
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
      uploadDate: '09/11/2023',
      enabled: !isStainDisabled(chunk.Stain) ? 'true' : 'false'
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

function submitStreamOld() {
  let buffer = []
  return through2.obj(async function (chunk, encoding, callback) {
    try {
      buffer.push(chunk)
      // If buffer is full, flush it
      if (buffer.length > flushThreshold) {
        await sendData(buffer)
        buffer = []
      }
      this.push(chunk)
      callback()
    } catch (e) {
      console.error(`Problem posting ${JSON.stringify(buffer)}`, e)
      callback(e)
    }
  }, flush(buffer))
}

function submitStream() {
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
          await sendData(buffer)
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
    }, flush(buffer)).on('finish', () => {
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

async function sendData(buffer) {
  await axiosClient.post(endpoint, buffer, {
    headers: {
      Authorization: 'Bearer eyJraWQiOiJ6SlZNOHkrV2QycVFuejZNZWt0cVZXQWxTRmw0UXJKUkFROFVcLzVcL3pTWnc9IiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiJmNGI4YzRhOC1iMDUxLTcwOWMtOWY1OC1lODFkMjQ0ZGE1NjciLCJjb2duaXRvOmdyb3VwcyI6WyJjaGFyY290LWFkbWluIl0sImlzcyI6Imh0dHBzOlwvXC9jb2duaXRvLWlkcC51cy1lYXN0LTEuYW1hem9uYXdzLmNvbVwvdXMtZWFzdC0xX2xldVcxc2FodiIsImNsaWVudF9pZCI6IjJrY2x1amJzaW9nbjQ3aHI3aGxtMTRobjBuIiwib3JpZ2luX2p0aSI6IjFiOTk2MzNkLTg3ZjMtNDJmZi04ZGI0LWQwMmRmODhhM2ZkMSIsImV2ZW50X2lkIjoiNzRhOWYwMTgtNjc0NS00NjExLTlkNWItYzZhMTg4ZDMzZDg0IiwidG9rZW5fdXNlIjoiYWNjZXNzIiwic2NvcGUiOiJhd3MuY29nbml0by5zaWduaW4udXNlci5hZG1pbiIsImF1dGhfdGltZSI6MTcyMDc5MTE5NiwiZXhwIjoxNzIwNzk0Nzk2LCJpYXQiOjE3MjA3OTExOTYsImp0aSI6IjczOThlOGI5LWY1ZDgtNGI5OC1iM2M4LWI4NTdlOWQ2YzZlMyIsInVzZXJuYW1lIjoiZjRiOGM0YTgtYjA1MS03MDljLTlmNTgtZTgxZDI0NGRhNTY3In0.UmD81L3ybQbwhgf65-GfNd4E5MHhKVWwu6hERgizmGoAYRaSuAYylloPNRSm4dfo8P-SQlPUf5xoZXV0_cn1oSm_F0fnQoYnCh3Q1S64T1CnSZgmp7J3yWFVw0jp7GsY4lGpquGHU1EIyzeACW5sul7oHVWHMOon0IXKyV6CWBE006TOnAQYRLskT_JxbGTvOm9138nP6aF0fRcISFmKCDjqAvXyEOXuqGkP6L5a9p5zzbPLXBzt24R80E0w7CIMXrBWrbMP5DOaGtE4egQOiSeLVRO-c9sEydlxyWzblwMJXzRix2UUxv46PukuDluArHHRR7cREdsI3NxWP40Mwg'
    }
  })
  // console.log(`JMQ: Successfully posted ${JSON.stringify(buffer)}`)
  return Promise.resolve()
}
