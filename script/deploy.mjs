#!/usr/bin/env zx

const fs = require('fs')
const yargs = require('yargs/yargs')
const path = require('path')

process.env.IS_DEPLOY_SCRIPT = 1
/**
 * Use this script and this script only to deploy. The reason is that the
 * the stacks are split across two different AWS accounts (Paid and ODP). SST
 * disallows use of 'env' to specify target account/region for a stack. Instead account/region come from
 * the passed in AWS CLI counterparts (E.g. AWS_PROFILE=... or 'aws --profile ...'. This script
 * allows one to specify the Mt Sinai paid and ODP account AWS profiles individually.<br/>
 * Must still use the 'npx sst start..' command to start the debug environment, which will
 * deploy all stacks to the same account. This simplifies things when it comes to debugging, for
 * we can then use a single node process to test flows that would otherwise do cross-account access,
 * for example image transfer lambda.
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
  .alias('c', 'cleanup')
  .describe('p', 'Paid account AWS profile')
  .describe('o', 'ODP account AWS profile')
  .describe('s', 'Stage to deploy to')
  .describe('c', 'Whether to cleanup after  removal. WARNING: BE CAREFUL in PROD!!!!')
  .demandCommand(2, 2, 'Specify either start or deploy')
  .demandOption(['p', 'o', 's'])
  .boolean('c')
  .help('h')
  .alias('h', 'help')
  .argv

const action = argv._[1]

const { paidAccountProfile, odpAccountProfile, stage, cleanup } = argv
const commandObjs = [
  {
    profile: paidAccountProfile,
    stack: 'common'
  },
  {
    profile: paidAccountProfile,
    stack: 'backend-paid-account',
  },
  {
    profile: paidAccountProfile,
    stack: 'fulfillment',
  },
  {
    profile: odpAccountProfile,
    stack: 'backend-odp',
  },
  {
    profile: paidAccountProfile,
    stack: 'frontend',
  }
]

process.chdir(path.resolve(__dirname, '../'))
console.log(process.cwd())

try {
  if (action === 'remove') {
    // We're in "remove" mode, execute in reverse so that dependencies are removed last, else
    // AWS will crap out
    const numOfStacks = commandObjs.length
    for (let i = 0; i < numOfStacks; i++) {
      const obj = commandObjs.pop()
      process.env.AWS_PROFILE = obj.profile
      await $`env npx sst ${action} --stage=${stage} ${obj.stack}`
    }
    if (cleanup) {
      console.log('Executing post remove cleanup commands...')
      await $`./script/cleanup.sh ${stage} ${paidAccountProfile} ${odpAccountProfile}`
    }
  } else {
    // We're in "deploy" mode
    for (const obj of commandObjs) {
      process.env.AWS_PROFILE = obj.profile
      const res = await $`env npx sst ${action} --stage=${stage} ${obj.stack}`
      retrieveStackOutputsAndStoreInEnvironment(res)
    }

    // ...and update ODP image bucket policy to allow access for image transfer. This
    // step is needed in 'prod' stage only, read function doc above it.
    if (stage === 'prod') {
      await updateOdpCerebrumImageBucketPolicy({
        awsProfile: odpAccountProfile,
        bucket: process.env.CerebrumImageOdpBucketName,
        imageTransferLambdaRoleArn: process.env.HandleCerebrumImageTransferRoleArn,
        fulfillmentLambdaRoleArn: process.env.FulfillmentServiceTaskRoleArn
      })
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
                                                     imageTransferLambdaRoleArn,
                                                     fulfillmentLambdaRoleArn
                                                   }) {
  const policyAmendments = JSON.parse(`{"Version":"2012-10-17","Statement":[{"Sid":"charcot-image-transfer-put-object","Effect":"Allow","Principal":{"AWS":"${imageTransferLambdaRoleArn}"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::${bucket}/*"},{"Sid":"charcot-fulfillment-get-object","Effect":"Allow","Principal":{"AWS":"${fulfillmentLambdaRoleArn}"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::${bucket}/*"},{"Sid":"charcot-fulfillment-list-bucket","Effect":"Allow","Principal":{"AWS":"${fulfillmentLambdaRoleArn}"},"Action":"s3:ListBucket","Resource":"arn:aws:s3:::${bucket}"}]}`)

  // First get the current bucket policy...
  await $`AWS_PROFILE=${awsProfile} aws s3api get-bucket-policy --bucket ${bucket} --output text > /tmp/policy.json`
  const currentBucketPolicy = JSON.parse(fs.readFileSync('/tmp/policy.json'))

  const newPolicy = currentBucketPolicy

  /*
   * Do this to make this operation idempotent. The Set will dedup
   * so we don't insert duplicates when deploy is run multiple times.
   * Note: AWS replaces the policy principal with a token in scenarios where the principal resource has been deleted,
   *   hence the reason for filtering out based on the 'sid' to avoid errors during policy update. See https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_principal.html,
   *   search for "When this happens, the principal ID appears in resource-based policies because AWS can no longer map it back to a valid ARN"

   */
  const policyStatements = new Set()
  for (const statement of currentBucketPolicy.Statement.filter(e => !e.Sid || !e.Sid.startsWith('charcot-')).concat(policyAmendments.Statement)) {
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
 * Have to do this to then pass 'env' output in front of the AWS command (E.g. 'env aws ...'). Prepending each environment
 * var separately to the script like '${varAndVal}' where valAndVal = VAR=VAL does not cut it because zx
 * automatically adds quotes, producing $'VAR=VAL', which doesn't pass the env values as expected. Read
 * all about it at https://github.com/google/zx/blob/main/docs/quotes.md
 */
function retrieveStackOutputsAndStoreInEnvironment (stackOutput) {
  const matches = stackOutput.toString().matchAll(/\s{4}(\S+): (.+)/g)
  // Set up environment needed by ODP stack deploy
  for (const m of matches) {
    const key = m[1]
    const val = m[2]
    process.env[key] = val
    console.log(`Set environment value ${key}=${val}`)
  }
}
