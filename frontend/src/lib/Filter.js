export default class Filter {
  constructor () {
    this.filter = {}
  }

  add ({ dimension, category }) {
    let categories = this.filter[dimension]
    if (!categories) {
      categories = new Set()
      this.filter[dimension] = categories
    }
    categories.add(category)
    return this
  }

  clear () {
    this.filter = {}
    return this
  }

  clone () {
    const clone = new Filter()
    for (const tup of Object.entries(this.filter)) {
      tup[1].forEach((val) => clone.add({ dimension: tup[0], category: val }))
    }
    return clone
  }

  has ({ dimension, category }) {
    const categories = this.filter[dimension]
    return categories && categories.has(category)
  }

  isEmpty() {
    return Object.keys(this.filter).length < 1
  }

  jsx () {
    return <div/>
  }

  remove ({ dimension, category }) {
    this.filter[dimension].delete(category)

    // Delete this dimension from the object if
    // this was the only selected category
    if (!this.filter[dimension].size) {
      delete this.filter[dimension]
    }
    return this
  }

  serialize (dimensionToIgnore) {
    const filterStr = Object.entries(this.filter).filter((tup) => tup[0] !== dimensionToIgnore).map(tup => {
      const predicates = Array.from(tup[1].values()).map(val => `${tup[0]} = '${val.replace(/'/g, '__QUOTE__')}'`)
      const predicatesAsString = predicates.join(' OR ')
      return predicates.length > 1 ? `(${predicatesAsString})` : predicatesAsString
    })
    return filterStr.length > 0 ? `${filterStr.join(' AND ')}` : undefined
  }
}
