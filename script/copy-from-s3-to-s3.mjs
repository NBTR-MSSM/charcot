#!/usr/bin/env zx
const yargs = require('yargs/yargs')

/**
 * Transfer brain slides from one bucket to another within the same AWS account or across accounts.
 */

const argv = yargs(process.argv.slice(2))
  .usage('Usage: copy-from-s3-to-s3.mjs [options]')
  .example('copy-from-s3-to-s3.mjs -k <aws access key id of source account> -s <aws secret access key of source account> -f <from bucket> -p <from path/folder> -t <to bucket> [-d <to-path> -r <start after key>]', 'Sync files from source to destination AWS account S3 bucket')
  .alias('k', 'key-id')
  .alias('s', 'secret-key')
  .alias('f', 'from-bucket')
  .alias('p', 'from-path')
  .alias('t', 'to-bucket')
  .alias('d', 'to-path')
  .alias('r', 'start-after')
  .demandOption(['k', 's', 'f', 'p', 't'])
  .help('h')
  .alias('h', 'help')
  .argv

const { keyId: accessKeyId, secretKey: secretAccessKey, fromBucket, fromPath, toBucket, toPath, startAfter } = argv

const { s3Client } = require('@exsoinn/aws-sdk-wrappers')
const fromS3Client = s3Client.buildNewClient({
  accessKeyId,
  secretAccessKey
})
await s3Client.sync({ fromS3Client, fromBucket, fromPath, toBucket, toPath, startAfter})
