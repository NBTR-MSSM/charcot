#!/usr/bin/env zx

const yargs = require('yargs/yargs')

const { cloudFormationClient } = require('@exsoinn/aws-sdk-wrappers')

const argv = yargs(process.argv.slice(2))
  .usage('Usage: validate-charcot-active-iam-roles.mjs [options]')
  .example('copy-from-s3-to-s3.mjs -s <stage> -f <from bucket>', 'Checks IAM roles associated with CloudFormation stacks against roles listed by Mt Sinai as "inactive" that should be deleted. If there is a match, it means IM role IS active.')
  .alias('s', 'stage')
  .demandOption(['s'])
  .help('h')
  .alias('h', 'help')
  .argv

const { stage } = argv

const mtSinaiInactiveIamRoles = JSON.parse(fs.readFileSync(`${process.env.HOME}/Library/Application Support/JetBrains/WebStorm2024.1/scratches/aws-iam-roles-reported-by-mt-sinai.json`))
for (const stackName of [`${stage}-charcot-fulfillment`, `${stage}-charcot-common`, `${stage}-charcot-frontend`, `${stage}-charcot-backend-paid-account`]) {
  const res = await cloudFormationClient.describeStackResources({ StackName: stackName }).promise()
  for (const stackResource of res.StackResources.filter(stackResource => stackResource.ResourceType === 'AWS::IAM::Role')) {
    const charcotIamRole = `arn:aws:iam::045387143127:role/${stackResource.PhysicalResourceId}`
    const matchedIamRole = mtSinaiInactiveIamRoles[charcotIamRole]
    if (matchedIamRole) {
      console.log(`${JSON.stringify(stackResource, null, 2)}\n`)
      //console.log(`Found ${matchedIamRole}\n${JSON.stringify(stackResource, null, 2)}\n`)
    } else {
      console.log(`Not found ${charcotIamRole}`)
    }
  }
}
