import React, { Component } from 'react'
import { AppContext } from '../lib/context'
import './Transaction.css'
import { API } from 'aws-amplify'
import TransactionItem from '../components/TransactionItem'
import { InputGroup, Modal, Spinner, Table } from 'react-bootstrap'
import Pagination from 'react-bootstrap/Pagination'
import { BsArrowRepeat, BsSortDownAlt, BsSortUpAlt } from 'react-icons/bs'
import { DateTimeFormatter, LocalDateTime } from 'js-joda'
import Form from 'react-bootstrap/Form'
import debounce from 'lodash.debounce'

const formattedDateTime = () => LocalDateTime.now().format(DateTimeFormatter.ofPattern('yyyyMMdd-HH-mm-ss'))

class Transaction extends Component {
  constructor(props) {
    super(props)
    this.state = {
      orders: [],
      ordersSerialized: [],
      page: 1,
      isLoading: false,
      pageSize: 15,
      totalPages: 0,
      sortBy: 'created',
      sortOrder: 'desc',
      orderCount: 0,
      size: 0,
      slides: 0,
      uniqueUsers: 0
    }
  }

  async componentDidMount() {
    console.log('transaction mounted')
    this.context.pushToHistory()
    await this.retrieveOrders()
  }

  async componentDidUpdate(prevProps, prevState, snapshot) {
    if (this.state.page !== prevState.page || this.state.sortBy !== prevState.sortBy || this.state.sortOrder !== prevState.sortOrder) {
      console.log('transaction updated')
      await this.retrieveOrders()
    }
  }

  createDownloadUrl = () => {
    // eslint-disable-next-line no-undef
    return window.URL.createObjectURL(new Blob([this.state.ordersSerialized.join('\n')], { type: 'text/plain' }))
  }

  retrieveOrdersAsDelimiterSeparatedRecords = async () => {
    let ret = ['orderId,created,institutionName,email,filter,status,remark']
    const res = await this.fetchOrders({ page: -1 })
    ret = ret.concat(res.orders.map(order => {
      const {
        orderId,
        created,
        institutionName,
        email,
        filter,
        status,
        remark
      } = order
      return `${orderId},${created},${institutionName},${email},${filter},${status},${remark}`
    }))
    return ret
  }

  fetchOrders = async (queryParams) => {
    return await API.get('charcot', '/cerebrum-image-orders', {
      queryStringParameters: {
        pageSize: this.state.pageSize,
        page: this.state.page,
        sortBy: this.state.sortBy,
        sortOrder: this.state.sortOrder,
        ...queryParams
      }
    })
  }

  retrieveOrders = async () => {
    this.setState({
      isLoading: true
    })
    const res = await this.fetchOrders({})

    /*
     * TODO: Keep an eye on performance and if impacted, retrieve for-download orders ('ordersSerialized') on demand only,
     *  as opposed to everytime we render
     */
    this.setState({
      orders: res.orders,
      ordersSerialized: await this.retrieveOrdersAsDelimiterSeparatedRecords(),
      totalPages: res.totalPages,
      orderCount: res.orderCount,
      size: res.size,
      slides: res.slides,
      uniqueUsers: res.uniqueUsers,
      isLoading: false
    })
    this.context.handleTransactionUpdate({
      requests: this.state.orderCount,
      size: this.state.size,
      slides: this.state.slides,
      uniqueUsers: this.state.uniqueUsers
    })
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

  debouncedHandlePageSizeChange = debounce(this.retrieveOrders, 500)

  handlePageSizeChange = (event) => {
    const newState = {}
    const {
      id,
      value
    } = event.target
    console.log(`JMQ: id is ${id}, value is ${value}`)
    newState[id] = value
    this.setState(newState)
    this.debouncedHandlePageSizeChange()
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
        page = this.state.page <= 1 ? 1 : this.state.page - 1
        break
      case '›':
      case '›Next':
        page = this.state.page >= this.state.totalPages ? this.state.totalPages : this.state.page + 1
        break
      case '»':
      case '»Last':
        page = this.state.totalPages
        break
      default:
      // It's a number
    }

    this.setState({
      page: Number.parseInt(page)
    })
  }

  handleSort = (event) => {
    event.preventDefault()
    const { name: sortBy } = event.target
    const sortOrder = this.state.sortOrder === 'desc' ? 'asc' : 'desc'
    this.setState({
      sortBy,
      sortOrder
    })
  }

  renderPageSizeChangeForm = () => {
    return <Form>
      <Form.Group controlId="pageSize" size="sm">
        <InputGroup className="mb-3">
          <InputGroup.Text id="basic-addon1">Transactions per Page</InputGroup.Text>
          <Form.Control
            aria-describedby="basic-addon1"
            type="text"
            value={this.state.pageSize}
            onChange={this.handlePageSizeChange}
            onFocus={() => this.setState({ pageSize: '' })}
          />
        </InputGroup>
      </Form.Group>
    </Form>
  }

  renderPagination = () => {
    const items = []
    for (let number = 1; number <= this.state.totalPages; number++) {
      items.push(
        <Pagination.Item name={number} onClick={this.handlePageChange} key={number}
                         active={number === this.state.page}>
          {number}
        </Pagination.Item>
      )
    }

    const totalRecords = (
      <span>
        <span className="totalRecords">Total records: </span>
        <a href={this.createDownloadUrl()}
           download={`charcot-transactions-${formattedDateTime()}.csv`}>{this.state.orderCount} (Click to download as a plaintext CSV file)</a>
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
    if (field !== this.state.sortBy) {
      return <></>
    }

    return this.state.sortOrder === 'desc' ? <BsSortDownAlt/> : <BsSortUpAlt/>
  }

  renderLoaded = () => {
    const pagination = this.renderPagination()
    const pageSizeChangeForm = this.renderPageSizeChangeForm()
    return (<div className="Transaction">
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
          <th>Status</th>
        </tr>
        </thead>
        <tbody>
        {this.state.orders.map((e) => (<TransactionItem key={e.orderId} item={e}/>))}
        </tbody>
      </Table>
      {pagination}
    </div>)
  }

  render() {
    return (<div className="Transaction">{this.state.isLoading ? this.renderLoading() : this.renderLoaded()}</div>)
  }
}

Transaction.contextType = AppContext

export default Transaction
