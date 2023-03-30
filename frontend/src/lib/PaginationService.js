class PaginationService {
  goToPage (items, page, pageSize) {
    if (pageSize >= items.length) {
      return items
    }
    const first = (pageSize * page) - pageSize
    const last = (pageSize * page)
    return items.slice(first, last)
  }
}

export default new PaginationService()
