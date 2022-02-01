#!/usr/bin/env zx

const fs = require('fs')

/**
 * The inputs expected in this order are:
 * 1. The AWS_PROFILE of the Mt Sinai paid account
 * 2. The AWS_PROFILE of the Mt Sinai ODP account
 * 3. Stage name (dev, prod)
 */
const args = process.argv.slice(3)

if (args.length < 3) {
  console.error('Expecting at least 2 parameters: <AWS profile of paid account>, <AWS profile of ODP account>, <stage>')
  process.exit(-1)
}

const [profilePaidAccount, profileOdpAccount, stage] = args

try {
  // First deploy paid account stack
  const charcotStackDeployOutput = await $`AWS_PROFILE=${profilePaidAccount} npx sst deploy --stage=${stage} charcot-stack`
  retrieveStackOutputsAndStoreInEnvironment(charcotStackDeployOutput)

  // Now deploy ODP stack
  process.env.AWS_PROFILE = profileOdpAccount
  const charcotStackOdpDeployOutput = await $`env npx sst deploy --stage=${stage} charcot-stack-odp`

  // Finally update ODB image bucket policy to allow access for image transfer. This
  // step is needed in 'prod' stage only, read function doc above it.
  if (stage === 'prod') {
    retrieveStackOutputsAndStoreInEnvironment(charcotStackOdpDeployOutput)
    await updateOdpCerebrumImageBucketPolicy({
      awsProfile: profileOdpAccount,
      bucket: process.env.CerebrumImageOdpBucketName,
      imageTransferLambdaRoleArn: process.env.HANDLE_CEREBRUM_IMAGE_TRANSFER_ROLE_ARN,
      fulfillmentLambdaRoleArn: process.env.HANDLE_CEREBRUM_IMAGE_FULFILLMENT_ROLE_ARN
    })
  }
} catch (e) {
  console.error(`Something went wrong: ${e}`)
}

/**
 * This is needed in the 'prod' stage only, to update existing image bucket
 * policy. Currently CDK does not support updating permissions on existing
 * buckets.
 */
async function updateOdpCerebrumImageBucketPolicy ({
                                                     awsProfile,
                                                     bucket,
                                                     imageTransferLambdaRoleArn: imageTransferLambdaRoleArn,
                                                     fulfillmentLambdaRoleArn: fulfillmentLambdaRoleArn
                                                   }) {
  const policyTmpl = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"[IMAGE_TRANSFER_LAMBDA_ROLE_ARN]"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::[BUCKET]/*"},{"Effect":"Allow","Principal":{"AWS":"[FULFILLMENT_LAMBDA_ROLE_ARN]"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::[BUCKET]/*"},{"Effect":"Allow","Principal":{"AWS":"[FULFILLMENT_LAMBDA_ROLE_ARN]"},"Action":"s3:ListBucket","Resource":"arn:aws:s3:::[BUCKET]"}]}'
  const policyAmendments = JSON.parse(policyTmpl.replace(/\[BUCKET\]/g, bucket).replace(/\[IMAGE_TRANSFER_LAMBDA_ROLE_ARN\]/g, imageTransferLambdaRoleArn)
    .replace(/\[FULFILLMENT_LAMBDA_ROLE_ARN\]/g, fulfillmentLambdaRoleArn))

  // First get the current bucket policy...
  await $`AWS_PROFILE=${awsProfile} aws s3api get-bucket-policy --bucket ${bucket} --output text > /tmp/policy.json`
  const currentBucketPolicy = JSON.parse(fs.readFileSync('/tmp/policy.json'))

  const newPolicy = currentBucketPolicy

  // Do this to make this operation idempotent. The Set will dedup
  // so we don't insert duplicates when deploy is run multiple times.
  const policyStatements = new Set()
  for (const statement of currentBucketPolicy.Statement.concat(policyAmendments.Statement)) {
    policyStatements.add(JSON.stringify(statement))
  }

  // Now update the bucket policy, and put it right back
  newPolicy.Statement = []
  policyStatements.forEach((statement) => newPolicy.Statement.push(JSON.parse(statement)))
  fs.writeFileSync('/tmp/new-policy.json', JSON.stringify(newPolicy))
  await $`AWS_PROFILE=${awsProfile} aws s3api put-bucket-policy --bucket ${bucket} --policy file:///tmp/new-policy.json`
  console.log(`Updated bucket ${bucket} policy with ${JSON.stringify(newPolicy, null, ' ')}`)
}

function retrieveStackOutputsAndStoreInEnvironment (stackOutput) {
  const matches = stackOutput.toString().matchAll(/\s{4}(\S+): (.+)/g)
  // Set up environment needed by ODP stack deploy
  for (const m of matches) {
    const key = m[1].replace(/xUNDERx/g, '_')
    const val = m[2]
    process.env[key] = val
    console.log(`Set environment value ${key}=${val}`)
  }
}
