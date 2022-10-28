import React, { Component } from 'react'
import { AppContext } from '../lib/context'
import './Transaction.css'
import { API } from 'aws-amplify'
import TransactionItem from '../components/TransactionItem'
import { Modal, Spinner, Table } from 'react-bootstrap'
import Pagination from 'react-bootstrap/Pagination'
import { BsSortDownAlt, BsSortUpAlt, BsArrowRepeat } from 'react-icons/bs'

class Transaction extends Component {
  constructor(props) {
    super(props)
    this.state = {
      orders: [],
      page: 1,
      isLoading: false,
      pageSize: 15,
      totalPages: 0,
      sortBy: 'created',
      sortOrder: 'desc',
      orderCount: 0
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

  retrieveOrders = async () => {
    this.setState({
      isLoading: true
    })
    const res = await API.get('charcot', '/cerebrum-image-orders', {
      queryStringParameters: {
        pageSize: this.state.pageSize,
        page: this.state.page,
        sortBy: this.state.sortBy,
        sortOrder: this.state.sortOrder
      }
    })
    console.log(`JMQ: orders is ${JSON.stringify(res)}`)
    this.setState({
      orders: res.orders,
      totalPages: res.totalPages,
      orderCount: res.orderCount,
      isLoading: false
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

    console.log(`JMQ: event is ${page}`)
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

  renderPagination = () => {
    const items = []
    for (let number = 1; number <= this.state.totalPages; number++) {
      items.push(
        <Pagination.Item name={number} onClick={this.handlePageChange} key={number} active={number === this.state.page}>
          {number}
        </Pagination.Item>
      )
    }

    const totalRecords = <span><span className="totalRecords">Total records:</span> {this.state.orderCount}</span>
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

  renderLoaded = () => (
    <>
      {this.renderPagination()}
      <Table striped bordered hover>
        <thead>
        <tr>
          <th><a href="" onClick={this.handleSort} name="created">{this.renderSortIcon('created')}Request Date</a></th>
          <th><a href="" onClick={this.handleSort} name="requester">{this.renderSortIcon('requester')}Requester</a></th>
          <th><a href="" onClick={this.handleSort}
                 name="institutionName">{this.renderSortIcon('institutionName')}Institution</a></th>
          <th><a href="" onClick={this.handleSort} name="email">{this.renderSortIcon('email')}Email</a></th>
          <th>Criteria</th>
          <th>Status</th>
        </tr>
        </thead>
        <tbody>
        {this.state.orders.map((e) => (<TransactionItem key={e.orderId} item={e}/>))}
        </tbody>
      </Table>
      {this.renderPagination()}
    </>
  )

  render() {
    return (<div className="Transaction">{this.state.isLoading ? this.renderLoading() : this.renderLoaded()}</div>)
  }
}

Transaction.contextType = AppContext

export default Transaction
