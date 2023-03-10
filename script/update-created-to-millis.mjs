#!/usr/bin/env zx

/**
 * One-off created to update in DynamoDB order table the request create dates to be milliseconds since the epoch before
 * such change was made in the Charcot app. Initially Charcot was capturing the date in ISO standard format, but in order
 * to make sorting by date easier, decided to start capturing in milliseconds. Also the type of the 'created' field in DynamoDB
 * was changed from 'string' to 'number'. This script updates the type also. After this script runs, then can update
 * the DynamoDB order table 'created' field type to 'number' in the stack, and redeploy.
 */
const tableName = 'prod-charcot-cerebrum-image-order'
const res = await retrieveItems()
await updateItems(JSON.parse(res).Items)
// console.log(`JMQ: res is ${res}`)


/*
 * Functions
 */
function isNumber(number) {
  return number.match(/^\d+$/)
}

async function retrieveItems(stage) {
  return await $`aws --profile mssm dynamodb scan --table-name ${tableName}`
}

async function updateItems(items, stage) {
  for (const item of items) {
    const created = item.created.S
    // Skip the dates that were captured ***after*** the change from 'string' to 'number'
    if (created) {
      const createdAsMillis = new Date(isNumber(created) ? Number.parseInt(created) : created).getTime()
      const orderId = item.orderId.S
      console.log(`JMQ: updating item ${JSON.stringify(item)}, orderId ${orderId}, created ${created}, createdAsMillis ${createdAsMillis}`)
      await $`aws --profile mssm dynamodb update-item --table-name ${tableName} \
        --key '{"orderId": {"S":"${orderId}"}}' \
        --update-expression "SET #created = :created" \
        --expression-attribute-names '{"#created": "created"}' \
        --expression-attribute-values '{":created": {"N":"${createdAsMillis}"}}'`
    }
  }
}


