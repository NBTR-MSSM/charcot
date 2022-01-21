#!/usr/bin/env zx

const args = process.argv.slice(3)

if (args.length < 3) {
  console.error('Expecting at least 2 parameters: <AWS profile of paid account>, <AWS profile of ODP account>, <stage>')
  process.exit(-1)
}

const [profilePaidAccount, profileOdpAccount, stage] = args

try {
  // First deploy paid account stack
  const charcotStackDeployOutput = await $`AWS_PROFILE=${profilePaidAccount} npx sst deploy --stage=${stage} charcot-stack`
  const matches = charcotStackDeployOutput.toString().matchAll(/\s{4}(\S+): (.+)/g)

  // Set up environment needed by ODP stack deploy
  for (const m of matches) {
    const key = m[1].replace(/xUNDERx/g, '_')
    const val = m[2]
    process.env[key] = val
    console.log(`Set environment value ${key}=${val}`)
  }

  // Now deploy ODP stack
  process.env.AWS_PROFILE = profileOdpAccount
  await $`env npx sst deploy --stage=${stage} charcot-stack-odp`
} catch (e) {
  console.error(`Something went wrong: ${e}`)
}
