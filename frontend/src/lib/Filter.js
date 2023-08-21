/**
 * Internally the filter is maintained as an object where keys are dimension names and
 * values are Set's, where each set is a list of categories the user has selected for
 * that dimension.
 */
export default class Filter {
  constructor() {
    this.filter = {}
  }

  add({ dimension, category }) {
    let { categories } = this.filter[dimension] || { categories: undefined }
    if (!categories) {
      categories = new Set()
      this.filter[dimension] = {
        categories
      }
    }
    // Store all values as string to avoid to finding values because they're stored
    // as number and caller trying to retrieve as string
    categories.add(String(category))
    return this
  }

  categories({ dimension }) {
    return Array.from(this.filter[dimension].categories.values())
  }

  clear() {
    this.filter = {}
    return this
  }

  clone() {
    const clone = new Filter()
    for (const tup of Object.entries(this.filter)) {
      tup[1].categories.forEach((val) => clone.add({ dimension: tup[0], category: val }))
    }
    return clone
  }

  /**
   * Checks if the given dimension has the specified category, returning true if it does. If
   * category argument is undefined, this method assumes that caller wants to only check if the
   * dimension exists in the filter, in which case it returns true if it does also.
   */
  has({ dimension, category }) {
    const { categories } = this.filter[dimension] || { categories: undefined }
    return categories && (!category || categories.has(String(category)))
  }

  isEmpty() {
    return Object.keys(this.filter).length < 1
  }

  jsx() {
    return Object.entries(this.filter).map(tup => {
      const dimension = tup[0]
      const { categories } = tup[1]
      return Array.from(categories.values()).map(category => ({
        dimension,
        category
      }))
    }).flat()
  }

  remove({ dimension, category }) {
    const { categories } = this.filter[dimension]
    categories.delete(String(category))

    // Delete this dimension from the object if
    // this was the only selected category
    if (!categories.size) {
      delete this.filter[dimension]
    }
    return this
  }

  serialize(dimensionToIgnore) {
    const dimensionPredicates = Object.entries(this.filter).filter((tup) => tup[0] !== dimensionToIgnore).map(tup => {
      return this.serializeCategories(tup[0])
    })
    return dimensionPredicates.length > 0 ? `${dimensionPredicates.join(' AND ')}` : undefined
  }

  serializeCategories(dimension) {
    if (!this.filter[dimension]) {
      return ''
    }
    const categoryPredicates = Array.from(this.categories({ dimension })).map(val => `${dimension} = '${!Number.isInteger(val) ? val.replace(/'/g, '__QUOTE__') : val}'`)
    const categoryPredicatesAsString = categoryPredicates.join(' OR ')
    return categoryPredicates.length > 1 ? `(${categoryPredicatesAsString})` : categoryPredicatesAsString
  }
}
