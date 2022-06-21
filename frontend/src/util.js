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
