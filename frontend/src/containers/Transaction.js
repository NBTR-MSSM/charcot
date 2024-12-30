import React, { Component } from 'react'
import { AppContext } from '../lib/context'
import './Transaction.css'
import { API, Auth } from 'aws-amplify'
import TransactionItem from '../components/TransactionItem'
import { InputGroup, Modal, Spinner, Table } from 'react-bootstrap'
import Pagination from 'react-bootstrap/Pagination'
import { BsArrowRepeat, BsSortDownAlt, BsSortUpAlt } from 'react-icons/bs'
import { DateTimeFormatter, LocalDateTime, ZoneOffset } from 'js-joda'
import Form from 'react-bootstrap/Form'
import debounce from 'lodash.debounce'
import paginationService from '../lib/PaginationService'
import sortService from '../lib/SortService'

let persistedState = {
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

const formattedDateTime = () => LocalDateTime.now().format(DateTimeFormatter.ofPattern('yyyyMMdd-HH-mm-ss'))
const shouldReloadTransactions = () => new URLSearchParams(window.location.search).get('reload')

class Transaction extends Component {
  constructor(props) {
    super(props)
    this.state = {
      ...persistedState
    }
  }

  async componentDidMount() {
    console.log('JMQ: Transaction.componentDidMount()')
    // Request approval feature is for logged in Admin's, enforce that here. Recall
    // that CharcoRoutes.js relaxes access to /transaction, thus it is up to this component to
    // enforce it here. Refer to CharcoRoutes.js for the use-case that prompted this.
    const orderId = new URLSearchParams(window.location.search).get('orderId')
    const isOrderApproval = new URLSearchParams(window.location.search).get('orderApproval')
    if (orderId && isOrderApproval) {
      console.log('JMQ: Transaction.componentDidMount() about to call App.pushToHistory()')
      this.context.pushToHistory()
      if (!this.context.isAuthenticated) {
        this.context.redirect({ to: '/login' })
        return
      } else if (!this.context.isAdmin) {
        this.context.redirect({ to: '/not-found' })
        return
      }
    }
    if (!persistedState.initialOrderRetrieveHappened || shouldReloadTransactions()) {
      await this.retrieveOrders()
      persistedState.initialOrderRetrieveHappened = true
    }
    this.refreshSelectedOrders()
    // Redirect to request approval screen. CharcotRoutes.js enforces that only Admin's have access
    // to this feature.
    if (orderId && isOrderApproval) {
      this.context.handleSetTransactionItem(persistedState.orders.find(order => order.orderId === orderId))
      this.context.redirect({ to: '/transaction-detail' })
    }
  }

  createDownloadUrl = () => {
    // eslint-disable-next-line no-undef
    return window.URL.createObjectURL(new Blob([persistedState.ordersSerialized.join('\n')], { type: 'text/plain' }))
  }

  retrieveOrdersAsDelimiterSeparatedRecords = async () => {
    let ret = ['REQUEST ID,REQUESTER,CREATED,INSTITUTION NAME,EMAIL,SIZE (GB),SLIDE COUNT,STATUS,INTENDED USE,FILTER']
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
        filter,
        intendedUse
      } = order
      return `${orderId},${requester},${LocalDateTime.ofEpochSecond(Number.parseInt(created / 1000), ZoneOffset.UTC).format(DateTimeFormatter.ofPattern('MM/dd/yyyy HH:mm:ss'))} GMT,${institutionName},${email},${Number.parseFloat(size / Math.pow(2, 30)).toFixed(2)},${fileCount},${status},${intendedUse},${filter}`
    }))
    return ret
  }

  fetchOrders = async (queryParams) => {
    return await API.get('charcot', '/cerebrum-image-orders', {
      headers: {
        Authorization: `Bearer ${(await Auth.currentSession())
          .getAccessToken()
          .getJwtToken()}`
      },
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
    persistedState = {
      ...persistedState,
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
      requests: persistedState.orderCount,
      size: persistedState.size,
      slides: persistedState.slides,
      uniqueUsers: persistedState.uniqueUsers
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
    pageSize = persistedState.pageSize,
    page = persistedState.page
  } = {}) => {
    pageSize = pageSize < 1 ? 10 : pageSize
    page = page < 1 ? 1 : page
    persistedState = {
      ...persistedState,
      pageSize,
      page,
      totalPages: Math.ceil(persistedState.orderCount / pageSize)
    }
    this.setState({
      page: persistedState.page,
      pageSize: persistedState.pageSize,
      totalPages: persistedState.totalPages
    })
  }

  applySearchTerm = searchTerm => {
    const trimmedSearchTerm = searchTerm && searchTerm.trim()
    persistedState = {
      ...persistedState,
      searchTerm: trimmedSearchTerm
    }
    this.setState({
      searchTerm: persistedState.searchTerm
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
        page = persistedState.page <= 1 ? 1 : persistedState.page - 1
        break
      case '›':
      case '›Next':
        page = persistedState.page >= persistedState.totalPages ? persistedState.totalPages : persistedState.page + 1
        break
      case '»':
      case '»Last':
        page = persistedState.totalPages
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
    const sortOrder = persistedState.sortOrder === 'desc' ? 'asc' : 'desc'
    persistedState = {
      ...persistedState,
      sortBy,
      sortOrder
    }
    this.setState({
      sortBy: persistedState.sortBy,
      sortOrder: persistedState.sortOrder,
      orders: sortService.sort(persistedState.orders, persistedState.sortBy, persistedState.sortOrder)
    })
    this.refreshSelectedOrders()
  }

  refreshSelectedOrders = () => {
    this.debouncedUpdateSelectedOrders()
  }

  updateSelectedOrders = () => {
    // First navigate to page based on user page selections, then apply search term, if any
    let selectedOrders = paginationService.goToPage(persistedState.orders, persistedState.page, persistedState.pageSize)
    selectedOrders = persistedState.searchTerm ? selectedOrders.filter((e) => `${e.email}${e.institutionName}${e.requester}${e.status}${e.orderId}`.match(new RegExp(persistedState.searchTerm, 'i'))) : selectedOrders
    persistedState = {
      ...persistedState,
      selectedOrders
    }
    this.setState({
      selectedOrders: persistedState.selectedOrders
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
            value={persistedState.pageSize}
            onChange={this.handlePageSizeChange}
            onFocus={() => {
              persistedState = {
                ...persistedState,
                pageSize: ''
              }
              this.setState({ pageSize: persistedState.pageSize })
            }}
          />
        </InputGroup>
        <Form.Group controlId="searchTerm" size="sm">
          <InputGroup className="mb-3">
            <InputGroup.Text id="basic-addon1">Search Term</InputGroup.Text>
            <Form.Control
              aria-describedby="basic-addon1"
              type="text"
              value={persistedState.searchTerm}
              onChange={this.handleSearchTermChange}/>

            {persistedState.searchTerm
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
    for (let number = 1; number <= persistedState.totalPages; number++) {
      items.push(
        <Pagination.Item name={number} onClick={this.handlePageChange} key={number}
                         active={number === persistedState.page}>
          {number}
        </Pagination.Item>
      )
    }

    const totalRecords = (
      <span>
              <span className="totalRecords">Total records: </span>
              <a href={this.createDownloadUrl()}
                 download={`charcot-transactions-${formattedDateTime()}.csv`}>{persistedState.orderCount} (Click to download as a plaintext CSV file)</a>
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
    if (field !== persistedState.sortBy) {
      return <></>
    }

    return persistedState.sortOrder === 'desc' ? <BsSortDownAlt/> : <BsSortUpAlt/>
  }

  renderLoaded = () => {
    const pagination = this.renderPagination()
    const pageSizeChangeForm = this.renderControlForm()
    const orders = persistedState.selectedOrders || persistedState.orders
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
    return this.state.isLoading ? this.renderLoading() : this.renderLoaded()
  }
}

Transaction.contextType = AppContext

export default Transaction
