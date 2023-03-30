class SortService {
  sort(items, sortBy, sortOrder) {
    const comparator = (a, b, field = sortBy) => {
      const left = a[field]
      const right = b[field]

      // Determine if we're sorting numeric or string values. For everything else
      // we don't support sorting - just return 0
      let ret = 0
      if (typeof left === 'number' && typeof right === 'number') {
        ret = sortOrder === 'desc' ? right - left : left - right
      } else if (typeof left === 'string' && typeof right === 'string') {
        let strOne = left.toLowerCase()
        let strTwo = right.toLowerCase()
        if (sortOrder === 'desc') {
          strOne = right.toLowerCase()
          strTwo = left.toLowerCase()
        }
        if (strOne < strTwo) {
          ret = -1
        } else if (strOne > strTwo) {
          ret = 1
        } else {
          ret = 0
        }
      } else {
        ret = 0
      }
      // If we have a tie, use create timestamp to break it
      return ret === 0 ? comparator(a, b, 'created') : ret
    }

    return items.sort(comparator)
  }
}

export default new SortService()
