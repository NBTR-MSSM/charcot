import React, { Component } from 'react'
import { AppContext } from '../lib/context'
import './Transaction.css'
import { API } from 'aws-amplify'
import TransactionItem from '../components/TransactionItem'
import { InputGroup, Modal, Spinner, Table } from 'react-bootstrap'
import Pagination from 'react-bootstrap/Pagination'
import { BsArrowRepeat, BsSortDownAlt, BsSortUpAlt } from 'react-icons/bs'
import { DateTimeFormatter, LocalDateTime, ZoneOffset } from 'js-joda'
import Form from 'react-bootstrap/Form'
import debounce from 'lodash.debounce'
import paginationService from '../lib/PaginationService'
import sortService from '../lib/SortService'

const formattedDateTime = () => LocalDateTime.now().format(DateTimeFormatter.ofPattern('yyyyMMdd-HH-mm-ss'))

let savedState = {
  orders: [],
  selectedOrders: undefined,
  ordersSerialized: [],
  page: 1,
  isLoading: false,
  pageSize: 10,
  totalPages: 0,
  sortBy: 'created',
  sortOrder: 'desc',
  orderCount: 0,
  size: 0,
  slides: 0,
  uniqueUsers: 0,
  searchTerm: '',
  initialOrderRetrieveHappened: false
}
class Transaction extends Component {
  constructor(props) {
    super(props)
    this.state = {
      ...savedState
    }
  }

  async componentDidMount() {
    this.context.pushToHistory()
    if (!savedState.initialOrderRetrieveHappened) {
      await this.retrieveOrders()
      savedState.initialOrderRetrieveHappened = true
    }
    this.refreshSelectedOrders()
  }

  createDownloadUrl = () => {
    // eslint-disable-next-line no-undef
    return window.URL.createObjectURL(new Blob([savedState.ordersSerialized.join('\n')], { type: 'text/plain' }))
  }

  retrieveOrdersAsDelimiterSeparatedRecords = async () => {
    let ret = ['REQUEST ID,REQUESTER,CREATED,INSTITUTION NAME,EMAIL,SIZE (GB),SLIDE COUNT,STATUS,FILTER']
    const res = await this.fetchOrders({ page: -1 })
    ret = ret.concat(res.orders.map(order => {
      const {
        orderId,
        requester,
        created,
        institutionName,
        email,
        size,
        fileCount,
        status,
        filter
      } = order
      return `${orderId},${requester},${LocalDateTime.ofEpochSecond(Number.parseInt(created / 1000), ZoneOffset.UTC).format(DateTimeFormatter.ofPattern('MM/dd/yyyy HH:mm:ss'))} GMT,${institutionName},${email},${Number.parseFloat(size / Math.pow(2, 30)).toFixed(2)},${fileCount},${status},${filter}`
    }))
    return ret
  }

  fetchOrders = async (queryParams) => {
    return await API.get('charcot', '/cerebrum-image-orders', {
      queryStringParameters: {
        page: -1,
        ...queryParams
      }
    })
  }

  retrieveOrders = async () => {
    this.setState({
      isLoading: true
    })

    // From fetchOrder response save to state only a few select properties,
    // because we don't want others to interfere with the client side pagination
    const res = await this.fetchOrders({})
    const { orders, totalPages, orderCount, size, slides, uniqueUsers } = res
    savedState = {
      ...savedState,
      ordersSerialized: await this.retrieveOrdersAsDelimiterSeparatedRecords(),
      orders,
      totalPages,
      orderCount,
      size,
      slides,
      uniqueUsers
    }

    /*
     * TODO: Keep an eye on performance and if impacted, retrieve for-download orders ('ordersSerialized') on demand only,
     *  as opposed to everytime we render Update 08/24/2023: This is mitigated by the fact that refresh is effectuated
     *  on demand only via the refresh icon button
     */
    this.setState({
      isLoading: false
    })
    this.context.handleTransactionUpdate({
      requests: savedState.orderCount,
      size: savedState.size,
      slides: savedState.slides,
      uniqueUsers: savedState.uniqueUsers
    })

    this.refreshSelectedOrders()
  }

  renderLoading = () => (
    <Modal
      show={true}
      size="xl"
      aria-labelledby="contained-modal-title-vcenter"
      centered>
      <Modal.Body>
        <center>
          <h3>Loading transactions...</h3>
          <Spinner animation="border" role="status">
            <span className="visually-hidden">Loading...</span>
          </Spinner>
        </center>
      </Modal.Body>
    </Modal>
  )

  debouncedRetrieveOrders = debounce(this.retrieveOrders, 500)

  handlePageSizeChange = async (event) => {
    this.updatePagination({
      page: 1,
      pageSize: event.target.value
    })
    this.refreshSelectedOrders()
  }

  updatePagination = ({
    pageSize = savedState.pageSize,
    page = savedState.page
  } = {}) => {
    pageSize = pageSize < 1 ? 10 : pageSize
    page = page < 1 ? 1 : page
    savedState = {
      ...savedState,
      pageSize,
      page,
      totalPages: Math.ceil(savedState.orderCount / pageSize)
    }
    this.setState({
      page: savedState.page,
      pageSize: savedState.pageSize,
      totalPages: savedState.totalPages
    })
  }

  applySearchTerm = searchTerm => {
    const trimmedSearchTerm = searchTerm && searchTerm.trim()
    savedState = {
      ...savedState,
      searchTerm: trimmedSearchTerm
    }
    this.setState({
      searchTerm: savedState.searchTerm
    })
  }

  handleSearchTermChange = async (event) => {
    const {
      value: searchTerm
    } = event.target
    this.applySearchTerm(searchTerm)
    this.refreshSelectedOrders()
  }

  handlePageChange = (event) => {
    event.preventDefault()
    let page = event.target.textContent
    switch (page) {
      case '«':
      case '«First':
        page = 1
        break
      case '‹':
      case '‹Previous':
        page = savedState.page <= 1 ? 1 : savedState.page - 1
        break
      case '›':
      case '›Next':
        page = savedState.page >= savedState.totalPages ? savedState.totalPages : savedState.page + 1
        break
      case '»':
      case '»Last':
        page = savedState.totalPages
        break
      default:
      // It's a number
    }

    this.updatePagination({ page: Number.parseInt(page) })
    this.refreshSelectedOrders()
  }

  handleSort = (event) => {
    event.preventDefault()
    const { name: sortBy } = event.target
    const sortOrder = savedState.sortOrder === 'desc' ? 'asc' : 'desc'
    savedState = {
      ...savedState,
      sortBy,
      sortOrder
    }
    this.setState({
      sortBy: savedState.sortBy,
      sortOrder: savedState.sortOrder,
      orders: sortService.sort(savedState.orders, savedState.sortBy, savedState.sortOrder)
    })
    this.refreshSelectedOrders()
  }

  refreshSelectedOrders = () => {
    this.debouncedUpdateSelectedOrders()
  }

  updateSelectedOrders = () => {
    // First navigate to page based on user page selections, then apply search term, if any
    let selectedOrders = paginationService.goToPage(savedState.orders, savedState.page, savedState.pageSize)
    selectedOrders = savedState.searchTerm ? selectedOrders.filter((e) => `${e.email}${e.institutionName}${e.requester}${e.status}`.match(new RegExp(savedState.searchTerm, 'i'))) : selectedOrders
    savedState = {
      ...savedState,
      selectedOrders
    }
    this.setState({
      selectedOrders: savedState.selectedOrders
    })
  }

  debouncedUpdateSelectedOrders = debounce(this.updateSelectedOrders, 500)

  renderControlForm = () => (
    <Form>
      <Form.Group controlId="pageSize" size="sm">
        <InputGroup className="mb-3 transactions-per-page">
          <InputGroup.Text id="basic-addon1">Transactions per Page</InputGroup.Text>
          <Form.Control
            aria-describedby="basic-addon1"
            type="text"
            value={savedState.pageSize}
            onChange={this.handlePageSizeChange}
            onFocus={() => {
              savedState = {
                ...savedState,
                pageSize: ''
              }
              this.setState({ pageSize: savedState.pageSize })
            }}
          />
        </InputGroup>
        <Form.Group controlId="searchTerm" size="sm">
          <InputGroup className="mb-3">
            <InputGroup.Text id="basic-addon1">Search Term</InputGroup.Text>
            <Form.Control
              aria-describedby="basic-addon1"
              type="text"
              value={savedState.searchTerm}
              onChange={this.handleSearchTermChange}/>

            {savedState.searchTerm
              ? (<button className="search-term-clear-btn" onClick={(e) => {
                  e.preventDefault()
                  this.applySearchTerm('')
                  this.refreshSelectedOrders()
                }
            }>
              X
            </button>)
              : ''}
          </InputGroup>
        </Form.Group>
      </Form.Group>
    </Form>)

  renderPagination = () => {
    const items = []
    for (let number = 1; number <= savedState.totalPages; number++) {
      items.push(
        <Pagination.Item name={number} onClick={this.handlePageChange} key={number}
                         active={number === savedState.page}>
          {number}
        </Pagination.Item>
      )
    }

    const totalRecords = (
      <span>
              <span className="totalRecords">Total records: </span>
              <a href={this.createDownloadUrl()}
                 download={`charcot-transactions-${formattedDateTime()}.csv`}>{savedState.orderCount} (Click to download as a plaintext CSV file)</a>
              </span>
    )

    const reload = <span className="reload"><a href="" onClick={async (e) => {
      e.preventDefault()
      await this.retrieveOrders()
    }}><BsArrowRepeat size="30px"/></a></span>

    if (items.length < 2) {
      return <>{totalRecords}{reload}</>
    }

    return (
      <div>
        <Pagination>
          <Pagination.First onClick={this.handlePageChange}/>
          <Pagination.Prev onClick={this.handlePageChange}/>
          {items}
          <Pagination.Next onClick={this.handlePageChange}/>
          <Pagination.Last onClick={this.handlePageChange}/>
        </Pagination>
        {totalRecords}
        {reload}
      </div>
    )
  }

  renderSortIcon = (field) => {
    if (field !== savedState.sortBy) {
      return <></>
    }

    return savedState.sortOrder === 'desc' ? <BsSortDownAlt/> : <BsSortUpAlt/>
  }

  renderLoaded = () => {
    const pagination = this.renderPagination()
    const pageSizeChangeForm = this.renderControlForm()
    const orders = savedState.selectedOrders || savedState.orders
    return <div className="Transaction">
      {pageSizeChangeForm}
      {pagination}
      <Table striped bordered hover>
        <thead>
        <tr>
          <th><a href="" onClick={this.handleSort} name="created">{this.renderSortIcon('created')}Request Date</a>
          </th>
          <th><a href="" onClick={this.handleSort} name="requester">{this.renderSortIcon('requester')}Requester</a>
          </th>
          <th><a href="" onClick={this.handleSort}
                 name="institutionName">{this.renderSortIcon('institutionName')}Institution</a></th>
          <th><a href="" onClick={this.handleSort} name="email">{this.renderSortIcon('email')}Email</a></th>
          <th>Criteria</th>
          <th><a href="" onClick={this.handleSort} name="size">{this.renderSortIcon('size')}Size</a></th>
          <th><a href="" onClick={this.handleSort} name="fileCount">{this.renderSortIcon('fileCount')}Slide Count</a>
          </th>
          <th>Status</th>
        </tr>
        </thead>
        <tbody>
        {orders.map((e) => (<TransactionItem key={e.orderId} item={e}/>))}
        </tbody>
      </Table>
      {pagination}
    </div>
  }

  render() {
    return (<div className="Transaction">{this.state.isLoading ? this.renderLoading() : this.renderLoaded()}</div>)
  }
}

Transaction.contextType = AppContext

export default Transaction
