import { API, Auth } from 'aws-amplify'
import Filter from './Filter'
import SubjectNumberEntry from '../components/SubjectNumberEntry'
import SexStatCustomDisplay from '../components/SexStatCustomDisplay'

// Our bonafide cache
const CACHE = new Map()

/*
 * Every time a chart needs to me modified and/or a new one added, add the corresponding config here
 */
const DIMENSION_CONFIGS = {
  subjectNumber: {
    name: 'subjectNumber',
    displayName: 'Subject Number',
    endpoint: '/cerebrum-images/subjectNumbers',
    statToDisplay: 'filteredCategoryCount',
    body: <SubjectNumberEntry/>,
    // use this dimension when determining if results found. Ideally only one dimension should bear this designation
    isResultsFoundDeterminant: true,
    // Subject Numbers is special in that it's not really a dimension, it serves more as a stat which number
    // selected varies depending on the filter of other dimension categories. For this reason we care only
    // about viewing a count of subject numbers that reflect the filtered results
    isViewFilteredOnly: true
  },
  age: {
    name: 'age',
    displayName: 'Age Group',
    endpoint: '/cerebrum-images/ages?interval=6&max=90&start=12',
    isNumeric: true
  },
  sex: {
    name: 'sex',
    displayName: 'Sex',
    endpoint: '/cerebrum-images/sexes',
    customStatDisplay: function (info) {
      return <SexStatCustomDisplay info={info}/>
    }
  },
  region: {
    name: 'region',
    displayName: 'Brain Region',
    endpoint: '/cerebrum-images/regions'
  },
  stain: {
    name: 'stain',
    displayName: 'Stain',
    endpoint: '/cerebrum-images/stains'
  },
  race: {
    name: 'race',
    displayName: 'Race',
    endpoint: '/cerebrum-images/races'
  },
  diagnosis: {
    name: 'diagnosis',
    displayName: 'Diagnosis',
    endpoint: '/cerebrum-images/diagnoses'
  }
}

/**
 * Calculate what the tick interval of the bars should
 * by taking average of the counts, then seeing if it's
 * in the 10's, or the 100's, or the 1000's, etc.
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
const retrieveData = async ({ config, dimension, filter, isAddMultiValueDimensionFilter }) => {
  const key = `${dimension}-${filter.serialize()}`
  if (!CACHE.has(key)) {
    CACHE.set(key, await API.get('charcot', config.endpoint, {
      headers: {
        Authorization: `Bearer ${(await Auth.currentSession())
          .getAccessToken()
          .getJwtToken()}`
      },
      queryStringParameters: {
        filter: filter.serialize({ dimensionToIgnore: dimension, isAddMultiValueDimensionFilter }),
        numeric: config.isNumeric
      }
    }))
  }
  return CACHE.get(key)
}

/**
 * Produces search facets from the raw dimension query results. This method will aggregate the categories found, producing a count
 * for each group of unique categories found for the dimension in question.
 * The term "facets" is borrowed from Solr,https://solr.apache.org/guide/solr/latest/query-guide/faceting.html
 * @param config - Config data object for this dimension. See below for description of each attribute<br/>
 * @param dimension - The dimension in question (Age, Sex, Region, Stain, Race, Diagnosis)<br/>
 * @param filter - The current filter selected by the user in the UI<br/>
 * @param values - The raw data of dimension categories that came from the API, in the form of an array of JSON objects<br/>
 * @param countHandling - When an initialCategories Map is specified, three possible values define the strategy to use to
 *   merge counts of categories between initialCategories Map and the passed in API result categories:<br/><br/>
 *     If category exists in both result sets:<br/>
 *       1. 'sum': Add them. The use case to which this logic apply is numeric categories that are grouped into ranges, where the same result set will contain the range repeated
 *         multiple times, and we have to aggregate the ranges into a sum per range group. Not currently in use, the use case for this came in error
 *         and has gone away, specifically related to displaying correct counts of AND'ed categories in the chart UI<br/>
 *       2. 'override': Override initialCategories count with count from the passed in results set's<br/>
 *       3. undefined: Passed in results category count inherits the initialCategories count. This is so that subsequent filter-less queries
 *         retain the count of the categories from the previous filtered query<br/>
 *     If category is not found in initialCategories<br/>
 *       4. N/A: Set count to zero regardless of countHandling passed, effectively ignoring countHandling. The use case here is to represent
 *          in the chart UI categories which did not match the previous filtered query, which would otherwise be absent from the chart UI
 *          if it wasn't for this logic.<br/>
 * @param initialCategories - Optional Map of categories from a previous query. These will get merged into the newly produced Map
 * @returns {{selectedCategories: Set<unknown>, selectedSlideCount: unknown, categories: *}} - A JSON object that contains these three fields:<br/>
 *   - selectedCategories: A set of the unique categories that user has selected in the UI<br/>
 *   - selectedSlideCount: The slide count that corresponds to the selected category(ies) for this dimension<br/>
 *   - categories: A Map keyed by category, where the value is a JSON object containing data such as the count for the category, etc.<br/>
 */
const prepareCategoryData = ({ config, dimension, filter, values, initialCategories = undefined, countHandling = undefined }) => {
  const categoryNameField = config.isNumeric ? 'range' : 'title'
  const categories = values.reduce((prev, cur) => {
    const currentCategory = {
      count: cur.count
    }

    currentCategory.name = cur[categoryNameField]

    const existingCategory = prev.get(currentCategory.name)
    if (existingCategory) {
      switch (countHandling) {
        case 'sum':
          currentCategory.count += existingCategory.count
          break
        case 'override':
          existingCategory.count = currentCategory.count
          break
        default:
          currentCategory.count = existingCategory.count
      }
    } else if (initialCategories) {
      // Previous query did not return this category as part of the filter, display it in the chart UI but with a 0 count as it did not match
      // the filter.
      currentCategory.count = 0
    }

    prev.set(currentCategory.name, currentCategory)

    if (filter.has({
      dimension,
      category: currentCategory.name
    })) {
      currentCategory.selected = true
    }

    return prev
  }, initialCategories || new Map())

  const selectedCategories = new Set(Array.from(categories.values()).filter(e => e.selected).map(e => e.name))
  return {
    categories,
    selectedCategories,
    selectedSlideCount: Array.from(categories.values()).filter(e => e.selected).reduce((prev, cur) => prev + cur.count, 0)
  }
}

/**
 * TODO: Use guava for cache management. Right now using a poor man's version that caches
 *       dimension-filter combo
 */
class DataService {
  async fetch({ dimension, filter }) {
    const config = DIMENSION_CONFIGS[dimension]
    const isFilterEmpty = filter.isEmpty()
    const {
      categories: filteredCategories,
      selectedCategories,
      selectedSlideCount
    } = prepareCategoryData({
      config,
      dimension,
      filter,
      countHandling: config.isNumeric ? 'sum' : undefined,
      values: await retrieveData({
        config,
        dimension,
        filter,
        isAddMultiValueDimensionFilter: true
      })
    })

    /*
     * Do a filter-less fetch to get super set of all the categories for the given dimension. The filtered list above
     * might have excluded dimensions/categories, yet we need them all available for user selection. This is just merely
     * to ensure that all dimension categories are present for selection (not just the selected ones) in the charts when
     * the user wants to update the filter.
     * (This applies only when AND'ing) Multi-value dimensions like Stain and Region is another use case where this is necessary,
     * with the difference that the filtered category counts ARE summed to the unfiltered ones. Why? Because we applied
     * the multi-value dimension filter to the dimension itself (see Filter.js serialize() method), which gives filtered counts,
     * in the chart for the dimension, but we need the full count w/o the filter as well to reflect accurate counts in
     * the chart of the UI.
     */
    // const isFilteringOnMultiValueDimension = filter.isFilteringOnMultiValueDimension({ dimension })
    let allCategories = filteredCategories
    if (!isFilterEmpty && !config.isViewFilteredOnly) {
      const unfilteredResults = await retrieveData({ config, dimension, filter: new Filter() })
      ;({ categories: allCategories } = prepareCategoryData({
        config,
        dimension,
        filter,
        values: unfilteredResults,
        // countHandling: isFilteringOnMultiValueDimension ? 'override' : undefined,
        initialCategories: allCategories
      }))
    }

    const categoryCount = Array.from(allCategories.keys()).length
    const chartHeight = categoryCount * 30
    return {
      dimension,
      displayName: config.displayName,
      categories: allCategories,
      selectedCategories,
      chartHeight: `${chartHeight < 200 ? 200 : (chartHeight > 600 ? 600 : chartHeight)}px`,
      expandable: chartHeight > 200,
      realHeight: `${chartHeight}px`,
      tickInterval: calculateTickInterval(allCategories),
      selectedCategoryCount: selectedCategories.size,
      selectedSlideCount,
      categoryCount,
      filteredCategoryCount: Array.from(filteredCategories.keys()).length,
      statToDisplay: config.statToDisplay,
      hideInAccordion: config.hideInAccordion,
      body: config.body,
      customStatDisplay: config.customStatDisplay,
      filteredCategories,
      isResultsFoundDeterminant: config.isResultsFoundDeterminant
    }
  }

  async fetchAll({ filter }) {
    const promises = []
    const dimensions = Object.keys(DIMENSION_CONFIGS)
    for (const dimension of dimensions) {
      promises.push(this.fetch({
        dimension,
        filter
      }))
    }
    const res = await Promise.all(promises)
    const ret = {
      dimensions: []
    }
    let selectedSlideCount = 0
    let isResultsFound = false
    for (const dimensionObj of res) {
      ret.dimensions.push(dimensionObj)
      // calculate a few running totals across all the dimensions
      selectedSlideCount = dimensionObj.selectedSlideCount || selectedSlideCount
      if (dimensionObj.isResultsFoundDeterminant) {
        isResultsFound = dimensionObj.filteredCategoryCount
      }
    }
    ret.selectedSlideCount = selectedSlideCount
    ret.isResultsFound = isResultsFound
    return ret
  }
}

export default new DataService()
