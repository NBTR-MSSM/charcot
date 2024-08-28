const { cloudFormationClient } = require('@exsoinn/aws-sdk-wrappers')

const mtSinaiInactiveIamRoles = JSON.parse(fs.readFileSync(`${process.env.HOME}/Library/Application Support/JetBrains/WebStorm2024.1/scratches/aws-iam-roles-reported-by-mt-sinai.json`))
for (const stackName of ['prod-charcot-fulfillment', 'prod-charcot-common', 'prod-charcot-frontend', 'prod-charcot-backend-paid-account']) {
  const res = await cloudFormationClient.describeStackResources({ StackName: stackName }).promise()
  for (const stackResource of res.StackResources.filter(stackResource => stackResource.ResourceType === 'AWS::IAM::Role')) {
    const charcotIamRole = `arn:aws:iam::045387143127:role/${stackResource.PhysicalResourceId}`
    const matchedIamRole = mtSinaiInactiveIamRoles[charcotIamRole]
    if (matchedIamRole) {
      console.log(`${JSON.stringify(stackResource, null, 2)}\n`)
      //console.log(`Found ${matchedIamRole}\n${JSON.stringify(stackResource, null, 2)}\n`)
    } else {
      //console.log(`Not found ${charcotIamRole}`)
    }
  }
}
