
class Pagination {
  /**
   * Given an array (0-based) of items, a page and a page size (I.e. number of items per page) returns the items in the array
   * in that page.
   * Example:
   *   13 items, pageSize = 5, page = 3
   *   first = (5 * 3) - 5 = 10
   *   last = (5 * 3) - (13 % 5) = 15 - 3 = 12
   *   (1-based) 1 2 3 4 5 6 7 8 9 10 11 12 13
   *   (0-based) 0 1 2 3 4 5 6 7 8 09 10 11 12
   *
   *  5 items, pageSize = 1, page = 2
   *   first = (1 * 2) - 1 = 1
   *   last = (1 * 2) - (5 % 1) = 2 - 0 = 2
   *   (1-based) 1 2 3 4 5 6 7 8 9 10 11 12 13
   *   (0-based) 0 1 2 3 4 5 6 7 8 09 10 11 12
   */
  static goToPage = <T>(items:T[], page: number, pageSize: number, itemCount: number): T[] => {
    // If page is not a positive value, or all items fit in a single page,
    // just grab all the records
    if (page < 1 || itemCount <= pageSize) {
      return items
    }
    const first = (pageSize * page) - pageSize
    const last = (pageSize * page)
    return items.slice(first, last)
  }
}

export default Pagination
