import React, { Component } from 'react'
import { AppContext } from '../lib/context'
import './Transaction.css'
import { API } from 'aws-amplify'
import TransactionItem from '../components/TransactionItem'
import { Modal, Spinner, Table } from 'react-bootstrap'
import Pagination from 'react-bootstrap/Pagination'

class Transaction extends Component {
  constructor(props) {
    super(props)
    this.state = {
      orders: [],
      page: 1,
      isLoading: false,
      pageSize: 5,
      totalPages: 0
    }
  }

  async componentDidMount() {
    console.log('transaction mounted')
    this.context.pushToHistory()
    this.updateIsLoadingState()
    await this.retrieveOrders()
    this.updateIsLoadedState()
  }

  updateIsLoadingState = () => {
    this.setState({
      isLoading: true
    })
  }

  updateIsLoadedState = () => {
    this.setState({
      isLoading: false
    })
  }

  async componentDidUpdate(prevProps, prevState, snapshot) {
    if (this.state.page !== prevState.page) {
      console.log('transaction updated')
      this.updateIsLoadingState()
      await this.retrieveOrders()
      this.updateIsLoadedState()
    }
  }

  retrieveOrders = async () => {
    const res = await API.get('charcot', '/cerebrum-image-orders', {
      queryStringParameters: {
        pageSize: this.state.pageSize,
        page: this.state.page
      }
    })
    this.setState({
      orders: res.orders,
      totalPages: res.totalPages
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

  renderPagination = () => {
    const items = []
    for (let number = 1; number <= this.state.totalPages; number++) {
      items.push(
        <Pagination.Item name={number} onClick={this.handlePageChange} key={number} active={number === this.state.page}>
          {number}
        </Pagination.Item>
      )
    }

    if (items.length < 2) {
      return <></>
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
      </div>
    )
  }

  renderLoaded = () => (
    <>
      {this.renderPagination()}
      <Table striped bordered hover>
        <thead>
        <tr>
          <th>Request ID</th>
          <th>Request Date</th>
          <th>Email</th>
          <th>Criteria</th>
          <th>User Info</th>
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
