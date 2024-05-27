#!/usr/bin/env zx

const fs = require('fs')

const records = JSON.parse(fs.readFileSync(`${process.env.HOME}/Library/Application Support/JetBrains/WebStorm2022.2/scratches/scratch-charcot-region-stain-anding.json`))

const res = {}
for (const obj of records) {
  // console.log(`JMQ: ${JSON.stringify(obj)}`)
  const subjectNumber = obj.subjectNumber.N
  let curSubjectNumberObject = res[subjectNumber]
  if (!curSubjectNumberObject) {
    curSubjectNumberObject = {}
    res[subjectNumber] = curSubjectNumberObject
  }
  for (const fld of ['region', 'stain']) {
    const fldVal = obj[fld].S
    curSubjectNumberObject[fld] = {
      ...(curSubjectNumberObject[fld] || {}),
      [fldVal]: 1 + ((curSubjectNumberObject[fld] && curSubjectNumberObject[fld][fldVal]) || 0)
    }
  }

}

console.log(`JMQ: res is ${JSON.stringify(res)}`)
