import React, { Component } from 'react'
import { AppContext } from '../lib/context'
import { Card, OverlayTrigger } from 'react-bootstrap'
import { BsInfoCircleFill } from 'react-icons/bs'
import {
  handleOrderApproval,
  orderApprovalConfirmationModal,
  successfulOrderApprovalConfirmationModal
} from '../lib/orderApproval'
import { handleOrderCancel, orderCancelConfirmationModal, successfulOrderCancellationConfirmationModal } from '../lib/orderCancel'

const attributeOrder = ['degree', 'institutionName', 'institutionAddress', 'areasOfInterest', 'intendedUse']

const persistedState = {
  // isShowSuccessfulOrderApprovalConfirmation: false,
  // isShowSuccessfulOrderCancellationConfirmation: false
}

class TransactionItem extends Component {
  constructor(props) {
    super(props)
    this.state = {
      // isShowOrderApprovalConfirmationModal: false,
      // isShowOrderCancelConfirmationModal: false,
      // Prevent impatient repeated clicks
      // isShowSuccessfulOrderCancellationConfirmation: false,
      // isDisableOrderCancelConfirmationModalButtons: false
    }
  }

  render() {
    const item = this.props.item

    const setStateFunction = this.setState.bind(this)
    const userAttributesPopover =
      <Card body style={{ width: '425px' }}>
      <span className="userAttribute"><span className="userAttributeName">Request ID</span>: {item.orderId}
        {item.isCancellable ? <a href="" onClick={(event) => handleOrderCancel(event, setStateFunction)}> Cancel</a> : <></>}
        {item.isApprovable ? <a href="" onClick={(event) => handleOrderApproval(event, setStateFunction)}> Approve</a> : <></>}
      </span>
        {attributeOrder.map(attrName => <span key={`${attrName}-${item.orderId}`} className="userAttribute"><span
          className="userAttributeName">{attrName}</span>: {item.userAttributes[attrName]}</span>)}
        <a href="" onClick={
          (e) => {
            e.preventDefault()
            this.context.handleSetOtherUserEmail(item.email)
            this.context.redirect({ to: '/edit-user' })
          }
        }>Update</a>
      </Card>

    return <tr>
      <td>{new Date(item.created).toUTCString()}</td>
      <td>
        <OverlayTrigger rootClose={true} trigger="click" placement="right" overlay={userAttributesPopover}>
          <a href="" onClick={(e) => e.preventDefault()}><BsInfoCircleFill/> {item.requester}</a>
        </OverlayTrigger>
        {orderApprovalConfirmationModal(this, this.context.email, item.orderId)}
        {successfulOrderApprovalConfirmationModal(item, this)}

        {orderCancelConfirmationModal(this, persistedState, this.context.email, item.orderId)}
        {successfulOrderCancellationConfirmationModal(item, this)}
      </td>
      <td>{item.institutionName}</td>
      <td>{item.email}</td>
      <td>{item.filter}</td>
      <td>{(item.size / Math.pow(2, 30)).toFixed(2)}GB</td>
      <td>{item.fileCount}</td>
      <td>
        <a href="" onClick={(e) => {
          e.preventDefault()
          this.context.handleSetTransactionItem(item)
          this.context.redirect({ to: '/transaction-detail' })
        }}>{item.status}</a>
      </td>
    </tr>
  }
}

TransactionItem.contextType = AppContext

export default TransactionItem
