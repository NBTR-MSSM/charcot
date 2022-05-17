import { API } from 'aws-amplify'

/**
 * Ensures that chart doesn't apply its own dimension filter, that's what the 'ignoreDimension' argument
 * is for. Charts should consume each others dimension filters but never their own.
 */
export const serializeFilter = (filterObject, ignoreDimension) => {
  const filterStr = Object.entries(filterObject).filter((tup) => tup[0] !== ignoreDimension).map(tup => {
    const predicates = Array.from(tup[1].values()).map(val => `${tup[0]} = '${val.replace(/'/g, '__QUOTE__')}'`)
    const predicatesAsString = predicates.join(' OR ')
    return predicates.length > 1 ? `(${predicatesAsString})` : predicatesAsString
  })
  return filterStr.length > 0 ? `${filterStr.join(' AND ')}` : undefined
}

/**
 * Invokes the passed in point which is assumed to be for obtaining dimension data,
 * and generates various stats like a count of image slides per category, grand total, etc.
 */
export const generateStats = async ({ endpoint, filter, dimension, isNumeric }) => {
  const values = await API.get('charcot', endpoint, {
    queryStringParameters: {
      filter: serializeFilter(filter, dimension)
    }
  })
  let grandTotal = 0
  const totalPerCategory = values.reduce((prev, cur) => {
    const cnt = cur.count
    grandTotal += cnt
    if (isNumeric) {
      const key = cur.range
      let total
      if (!(total = prev.get(key))) {
        prev.set(key, cnt)
      } else {
        prev.set(key, cnt + total)
      }
    } else {
      prev.set(cur.title, cnt)
    }

    return prev
  }, new Map())

  return { totalPerCategory, grandTotal }
}

/**
 * If this dimension's category is presently in the filter, then it means it's
 * selected.
 */
export const categoryIsSelected = ({ category, filter, dimension }) => {
  const categories = filter[dimension]
  return categories && categories.has(category)
}

export const countNumberOfCategories = ({ filter, dimension }) => {
  const categories = filter[dimension]
  return categories ? Array.from(categories.values()).length : 0
}

export const dimensionInfos = [
  { name: 'subjectNumber', displayName: 'Subject' },
  { name: 'age', displayName: 'Age Group' },
  { name: 'sex', displayName: 'Gender' },
  { name: 'region', displayName: 'Brain Region' },
  { name: 'stain', displayName: 'Stain' },
  { name: 'race', displayName: 'Race' },
  { name: 'disorder', displayName: 'Diagnosis' }]
