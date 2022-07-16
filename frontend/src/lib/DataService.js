import { API } from 'aws-amplify'
import Filter from './Filter'

/*
 * Every time a chart needs to me modified and/or a new one added, add the corresponding config here
 */
const DIMENSION_CONFIGS = {
  subjectNumber: {
    name: 'subjectNumber',
    displayName: 'Subject',
    endpoint: '/cerebrum-images/subjectNumbers',
    statToDisplay: 'filteredCategoryCount',
    hideInAccordion: true
  },
  age: {
    name: 'age',
    displayName: 'Age Group',
    endpoint: '/cerebrum-images/ages?interval=6&max=90&start=12',
    isNumeric: true
  },
  sex: { name: 'sex', displayName: 'Gender', endpoint: '/cerebrum-images/sexes' },
  region: { name: 'region', displayName: 'Brain Region', endpoint: '/cerebrum-images/regions' },
  stain: { name: 'stain', displayName: 'Stain', endpoint: '/cerebrum-images/stains' },
  race: { name: 'race', displayName: 'Race', endpoint: '/cerebrum-images/races' },
  diagnosis: { name: 'diagnosis', displayName: 'Diagnosis', endpoint: '/cerebrum-images/diagnoses' }
}

/**
 * Calculate what the tick interval of the bars should
 * by taking average of the counts, then seeing if it's
 * imn the 10's, or the 100's, or the 1000's, etc.
 * and coming up with a sensible interval. For example
 * if average is 1100, then tick interval is 1000.
 */
const calculateTickInterval = (categories) => {
  // Calculate average of counts
  const counts = Array.from(categories.values())
  const total = counts.reduce((prev, cur) => prev + cur.count, 0)
  const avg = total / counts.length
  return Math.pow(10, Math.trunc(Math.log10(avg)))
}

/**
 * Contacts endpoint which returns array of dimension/category data.
 */
const retrieveData = async ({ config, dimension, filter }) => {
  return await API.get('charcot', config.endpoint, {
    queryStringParameters: {
      filter: filter.serialize(dimension),
      numeric: config.isNumeric
    }
  })
}

/**
 * Produces a map of category name to category data object that includes the
 * count associated with the category, for example for the Gender dimension:
 *   Male   -> { count: 12345 }
 *   Female -> { count: 67890 }
 */
const prepareCategoryData = ({ config, dimension, filter, values, resetCountToZero = false }) => {
  let grandTotal = 0
  const selectedCategories = new Set()
  const categories = values.reduce((prev, cur) => {
    const currentCategory = {
      count: resetCountToZero ? 0 : cur.count
    }
    grandTotal += currentCategory.count
    if (config.isNumeric) {
      // Things like 'age' dimension will have the range repeated in the payload, because the ID is the age
      // but ranges group 2 or more age groups, hence the reason for logic below to keep track of ranges
      // seen thus far.
      currentCategory.name = cur.range
      let existingCategory
      if ((existingCategory = prev.get(currentCategory.name))) {
        currentCategory.count += existingCategory.count
      }
    } else {
      currentCategory.name = cur.title
    }

    prev.set(currentCategory.name, currentCategory)

    if (filter.has({ dimension, category: currentCategory.name })) {
      selectedCategories.add(currentCategory.name)
      currentCategory.selected = true
    }

    return prev
  }, new Map())

  return {
    categories,
    grandTotal,
    selectedCategories
  }
}

class DataService {
  async fetch ({ dimension, filter }) {
    const config = DIMENSION_CONFIGS[dimension]
    const filteredValues = await retrieveData({ config, dimension, filter })
    const { categories: filteredCategories, grandTotal, selectedCategories } = prepareCategoryData({
      config,
      dimension,
      filter,
      values: filteredValues
    })

    /*
     * Do a filter-less fetch to get super set of all the dimensions/categories. The filter
     * might have excluded dimensions/categories yet we need them all available for user selection
     */
    let unfilteredCategories = new Map()
    if (!filter.isEmpty()) {
      const unfilteredValues = await retrieveData({ config, dimension, filter: new Filter() })
      ;({ categories: unfilteredCategories } = prepareCategoryData({
        config,
        dimension,
        filter,
        values: unfilteredValues,
        resetCountToZero: true
      }))
    }

    const mergedCategories = new Map([...unfilteredCategories, ...filteredCategories])

    const categoryCount = Array.from(mergedCategories.keys()).length
    const chartHeight = categoryCount * 30
    return {
      dimension,
      displayName: config.displayName,
      categories: mergedCategories,
      grandTotal,
      selectedCategories,
      chartHeight: `${chartHeight < 200 ? 200 : (chartHeight > 600 ? 600 : chartHeight)}px`,
      tickInterval: calculateTickInterval(mergedCategories),
      selectedCategoryCount: selectedCategories.size,
      categoryCount,
      filteredCategoryCount: Array.from(filteredCategories.keys()).length,
      statToDisplay: config.statToDisplay,
      hideInAccordion: config.hideInAccordion
    }
  }

  async fetchAll ({ filter }) {
    const promises = []
    const dimensions = Object.keys(DIMENSION_CONFIGS)
    for (const dimension of dimensions) {
      promises.push(this.fetch({ dimension, filter }))
    }
    const res = await Promise.all(promises)
    const ret = {}
    for (let i = 0; i < res.length; i++) {
      ret[dimensions[i]] = res[i]
    }
    return ret
  }
}

export const dataService = new DataService()
