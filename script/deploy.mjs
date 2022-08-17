#!/usr/bin/env zx

const fs = require('fs')
const yargs = require('yargs/yargs')
const path = require('path')

process.env.IS_DEPLOY_SCRIPT = 1
/**
 * Use this script and this script only to deploy. The reason is that the
 * the stacks are split across two different AWS accounts (Paid and ODP). Not sure if it's
 * possible to change destination AWS account mid flight if we use the traditional
 * 'sst deploy...'  action.<br/>
 * Must still use the 'sst start..' to start the debug environment. This way only one node process
 * is spawned to handle requests for all the stacks, with the stacks getting deployed in the same account.
 *
 */
const argv = yargs(process.argv.slice(2))
  .usage('Usage: deploy.mjs <action> [options]')
  .command('deploy', 'Deploy stacks to the cloud')
  .command('remove', 'Remove the stacks from the cloud')
  .example('deploy.mjs deploy -p <paid account profile> -o <ODP account profile> -s <stage>', 'Deploy the stacks to the AWS cloud')
  .alias('p', 'paid-account-profile')
  .alias('o', 'odp-account-profile')
  .alias('s', 'stage')
  .describe('p', 'Paid account AWS profile')
  .describe('o', 'ODP account AWS profile')
  .describe('s', 'Stage to deploy to')
  .demandCommand(2, 2, 'Specify either start or deploy')
  .demandOption(['p', 'o', 's'])
  .help('h')
  .alias('h', 'help')
  .argv

const action = argv._[1]

const { paidAccountProfile, odpAccountProfile, stage } = argv
const commands = [
  {
    profile: paidAccountProfile,
    stack: 'backend-paid-account',
    command: `npx sst ${action} --stage=${stage}`
  },
  {
    profile: odpAccountProfile,
    stack: 'backend-odp',
    command: `npx sst ${action} --stage=${stage}`
  },
  {
    profile: paidAccountProfile,
    stack: 'frontend',
    command: `npx sst ${action} --stage=${stage}`
  }
]

process.chdir(path.resolve(__dirname, '../'))
console.log(process.cwd())

try {
  if (action === 'remove') {
    // We're in "remove" mode, execute in reverse so that dependencies are removed last, else
    // AWS will crap out
    const numOfStacks = commands.length
    for (let i = 0; i < numOfStacks; i++) {
      const obj = commands.pop()
      process.env.AWS_PROFILE = obj.profile
      await $`env npx sst ${action} --stage=${stage} ${obj.stack}`
    }
  } else {
    // We're in "deploy" mode
    // First deploy paid account stack
    for (const obj of commands) {
      process.env.AWS_PROFILE = obj.profile
      const res = await $`env npx sst ${action} --stage=${stage} ${obj.stack}`
      retrieveStackOutputsAndStoreInEnvironment(res)
    }

    // ...and update ODP image bucket policy to allow access for image transfer. This
    // step is needed in 'prod' stage only, read function doc above it.
    if (stage === 'prod') {
      /*await updateOdpCerebrumImageBucketPolicy({
        awsProfile: odpAccountProfile,
        bucket: process.env.CerebrumImageOdpBucketName,
        imageTransferLambdaRoleArn: process.env.HANDLE_CEREBRUM_IMAGE_TRANSFER_ROLE_ARN,
        fulfillmentLambdaRoleArn: process.env.HANDLE_CEREBRUM_IMAGE_FULFILLMENT_ROLE_ARN
      })*/
    }
  }
} catch (e) {
  console.error(`Something went wrong: ${e}`)
}

/**
 * This is needed in the 'prod' stage only, to update existing image bucket
 * policy to:
 * - Allow image transfer Lambda to put images in the ODP bucket
 * - Allow fulfillment Lambda to get/list images from image bucket in ODP account
 * Currently CDK does not support updating permissions on existing
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

/**
 * Have to do this to then pass 'env' action output to the AWS commands. Prepending each environment
 * var separately to the script like '${varAndVal}' where valAndVal = VAR=VAL does not cut it because zx
 * automatically adds quotes, producing $'VAR=VAL', which doesn't pass the env values as expected. Read
 * all about it at https://github.com/google/zx/blob/main/docs/quotes.md
 */
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
