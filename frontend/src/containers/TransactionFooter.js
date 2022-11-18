import React, { Component } from 'react'
import { AppContext } from '../lib/context'
import './TransactionFooter.css'
import Stack from 'react-bootstrap/Stack'

class TransactionFooter extends Component {
  render() {
    return (<footer className="TransactionFooter fixed-bottom">
      <Stack bsPrefix={'charcot-transaction-footer-hstack'} direction="horizontal" gap={3}>
        <span className="charcot-transaction-footer-stat"><span>{this.context.transactionData.requests}</span> Requests</span>
      </Stack>
    </footer>)
  }
}

TransactionFooter.contextType = AppContext

export default TransactionFooter
