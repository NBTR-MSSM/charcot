import { API } from 'aws-amplify'
import { serializeFilter } from '../util'

/*
 * Every time a chart needs to me modified and/or a new one added, add the corresponding coonfig here
 */
const DIMENSION_CONFIGS = {
  subjectNumber: {
    name: 'subjectNumber',
    displayName: 'Subject',
    endpoint: '/cerebrum-images/subjectNumbers',
    statToDisplay: 'totalCategories',
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

const categoryIsSelected = ({ category, filter, dimension }) => {
  const categories = filter[dimension]
  return categories && categories.has(category)
}

class DataService {
  async fetch ({ dimension, filter }) {
    const config = DIMENSION_CONFIGS[dimension]
    const values = await API.get('charcot', config.endpoint, {
      queryStringParameters: {
        filter: serializeFilter(filter, dimension),
        numeric: config.isNumeric
      }
    })
    let grandTotal = 0
    const selectedCategories = new Set()
    const categories = values.reduce((prev, cur) => {
      const currentCategory = {
        count: cur.count
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

      if (categoryIsSelected({ category: currentCategory.name, filter, dimension })) {
        selectedCategories.add(currentCategory.name)
        currentCategory.selected = true
      }

      return prev
    }, new Map())

    return {
      dimension,
      displayName: config.displayName,
      categories,
      grandTotal,
      selectedCategories,
      selectedCategoryCount: selectedCategories.size,
      totalCategories: Array.from(categories.keys()).length,
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
