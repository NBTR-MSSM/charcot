/**
 * Internally the filter is maintained as an object where keys are dimension names and
 * values are Set's, where each set is a list of categories the user has selected for
 * that dimension.
 */
const MULTI_VALUE_DIMENSIONS = ['region', 'stain']
export default class Filter {
  constructor() {
    this.filter = {}
  }

  add({
    dimension,
    category
  }) {
    let { categories } = this.filter[dimension] || { categories: undefined }
    if (!categories) {
      categories = new Set()
      this.filter[dimension] = {
        categories,
        logicalOperator: 'OR'
      }
    }
    // Store all values as string to avoid NOT finding values because they're stored
    // as number and caller trying to retrieve as string
    categories.add(String(category))

    this.updateMultiValueDimensions({ dimension })
    return this
  }

  categories({ dimension }) {
    return Array.from(this.filter[dimension].categories.values())
  }

  categoriesEscaped({ dimension }) {
    return this.categories({ dimension }).map(val => !Number.isInteger(val) ? val.replace(/'/g, '__QUOTE__') : val)
  }

  clear() {
    this.filter = {}
    return this
  }

  clone() {
    const clone = new Filter()
    for (const tup of Object.entries(this.filter)) {
      const dimension = tup[0]
      const dimensionObj = tup[1]
      dimensionObj.categories.forEach((category) => clone.add({
        dimension,
        category
      }))
      clone.filter[dimension].logicalOperator = dimensionObj.logicalOperator
    }
    return clone
  }

  /**
   * Checks if the given dimension has the specified category, returning true if it does. If
   * category argument is undefined, this method assumes that caller wants to only check if the
   * dimension exists in the filter, in which case it returns true if it does also.
   */
  has({
    dimension,
    category
  }) {
    const { categories } = this.filter[dimension] || { categories: undefined }
    return categories && (!category || categories.has(String(category)))
  }

  isEmpty() {
    return Object.keys(this.filter).length < 1
  }

  isFilterHasMultiValueDimension() {
    for (const dimension of MULTI_VALUE_DIMENSIONS) {
      if (this.isFilteringOnMultiValueDimension({
        dimension,
        isCheckLogicalOp: false
      })) {
        return true
      }
    }
    return false
  }

  isFilteringOnMultiValueDimension({
    dimension,
    isCheckLogicalOp = true
  }) {
    return (this.categoryLogicalOperator({ dimension }) === 'AND' || !isCheckLogicalOp) && this.filter[dimension] && this.filter[dimension].multiValueDimensionInfo && this.filter[dimension].multiValueDimensionInfo.value.length
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

  categoryLogicalOperator({ dimension }) {
    return this.filter[dimension] && this.filter[dimension].logicalOperator
  }

  remove({
    dimension,
    category
  }) {
    const { categories } = this.filter[dimension]
    categories.delete(String(category))

    // Delete this dimension from the object if
    // this was the only selected category
    if (!categories.size) {
      delete this.filter[dimension]
    } else if (categories.size < 2) {
      // if selected category is less than 2, than change to OR logical operator to
      // the multi value dimension filtering behavior which is relevant only when AND'ing 2 or
      // more categories
      this.filter[dimension].logicalOperator = 'OR'
    }
    this.updateMultiValueDimensions({ dimension })
    return this
  }

  serialize({
    dimensionToIgnore,
    isAddMultiValueDimensionFilter = false,
    isSubmission = false
  } = {}) {
    // When submitting the request, include all the dimensions in the filter, I.e. the filter is not being used to update the UI chars
    const dimensionPredicates = Object.entries(this.filter).filter((tup) => tup[0] !== dimensionToIgnore).map(tup => this.serializeCategories({
      dimension: tup[0],
      isSubmission
    }))
    /*
     * Add the multi value dimension predicate to filter for subjects that contain ALL of those categories. Applies only
     * when category predicates are using the AND logical op. The dimension will apply this filter to itself when updating
     * the charts in the UI
     */
    if (isAddMultiValueDimensionFilter && this.filter[dimensionToIgnore] && this.categoryLogicalOperator({ dimension: dimensionToIgnore }) === 'AND') {
      dimensionPredicates.push(this.serializeCategories({
        dimension: dimensionToIgnore,
        isAddMultiValueDimensionFilterOnly: true
      }))
    }
    return dimensionPredicates.length > 0 ? `${dimensionPredicates.join(' AND ')}` : undefined
  }

  serializeCategories({
    dimension,
    isAddMultiValueDimensionFilterOnly = false,
    isSubmission = false
  }) {
    if (!this.filter[dimension]) {
      return ''
    }

    const logicalOperator = this.categoryLogicalOperator({ dimension })

    // First obtain the category values with all special characters escaped
    const categories = this.categoriesEscaped({ dimension })

    // Now form predicates for each category value
    let categoryPredicates
    let multiValueDimensionInfo
    if (logicalOperator === 'AND' && (multiValueDimensionInfo = this.filter[dimension].multiValueDimensionInfo || isSubmission)) {
      /*
       * When AND'ing categories of eligible dimensions, the logic is to retrieve slides only from subjects which
       * contain ALL the categories being AND'ed together.
       */
      categoryPredicates = [...multiValueDimensionInfo.value]
      if (!isAddMultiValueDimensionFilterOnly) {
        categoryPredicates = [...categoryPredicates, `(${categories.map(val => `${dimension} = '${val}'`).join(' OR ')})`]
      }
    } else {
      categoryPredicates = categories.map(val => `${dimension} = '${val}'`)
    }
    const categoryPredicatesAsString = categoryPredicates.join(` ${logicalOperator} `)
    return categoryPredicates.length > 1 ? `(${categoryPredicatesAsString})` : categoryPredicatesAsString
  }

  toggleCategoryLogicalOperator({ dimension }) {
    this.filter[dimension].logicalOperator = this.filter[dimension].logicalOperator === 'OR' ? 'AND' : 'OR'
  }

  /**
   * When AND'ing categories, we search on a field that contains ALL the categories associated with
   * a subject. We form a query that will check that ALL categories are contained in such a field. On the server
   * side this field is stored as the subject categories concatenated into a single string, for example:
   * "allSubjectStains": "amyloidbeta||h&e||lfb-pas||modifiedbeilschowski||phosphorylatedtau||synuclein||ubiquitin"
   */
  updateMultiValueDimensions({ dimension }) {
    if (!this.filter[dimension] || !MULTI_VALUE_DIMENSIONS.includes(dimension)) {
      return
    }
    const field = `allSubject${dimension[0].toUpperCase()}${dimension.substring(1)}s`
    const value = this.categoriesEscaped({ dimension }).map(e => {
      return `contains(${field}, '${e.replace(/\s/g, '').toLowerCase()}')`
    })
    this.filter[dimension].multiValueDimensionInfo = {
      field,
      value
    }
  }
}
