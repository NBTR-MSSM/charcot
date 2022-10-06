import React, { Component } from 'react'
import { AppContext } from '../lib/context'
import './Transaction.css'
import { API } from 'aws-amplify'
import TransactionItem from '../components/TransactionItem'
import { Table } from 'react-bootstrap'

class Transaction extends Component {
  constructor(props) {
    super(props)
    this.state = {
      orders: [],
      page: 0
    }
  }

  async componentDidMount() {
    console.log('transaction mounted')
    this.context.pushToHistory()
    await this.retrieveOrders()
  }

  async componentDidUpdate(prevProps, prevState, snapshot) {
    if (this.state.page !== prevState.page) {
      console.log('transaction updated')
      await this.retrieveOrders()
    }
  }

  retrieveOrders = async () => {
    this.setState({
      orders: await API.get('charcot', '/cerebrum-image-orders', {})
    })
  }

  render() {
    return (
      <div className="Transaction">
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
      </div>
    )
  }
}

Transaction.contextType = AppContext

export default Transaction
