import React, { Component } from 'react'
import { AppContext } from '../lib/context'
import { Card, OverlayTrigger, Popover } from 'react-bootstrap'
import { BsInfoCircleFill } from 'react-icons/bs'
import { API } from 'aws-amplify'
import ConfirmationModal from './ConfirmationModal'
import Button from 'react-bootstrap/Button'

const attributeOrder = ['degree', 'institutionName', 'institutionAddress', 'areasOfInterest', 'intendedUse']

const savedState = {
  isShowSuccessfulOrderCancellationConfirmation: false
}

class TransactionItem extends Component {
  constructor(props) {
    super(props)
    console.log('JMQ: constructor()')
    this.state = {
      isShowOrderCancelConfirmModal: false,
      isDisableOrderCancelConfirmModalButtons: false,
      isShowSuccessfulOrderCancellationConfirmation: false
    }
  }

  handleOrderCancel = async (event) => {
    event.preventDefault()
    this.setState({
      isShowOrderCancelConfirmModal: true
    })
  }

  cancelOrder = async () => {
    await API.del('charcot', `/cerebrum-image-orders/${this.props.item.orderId}`, {
      queryStringParameters: {
        requester: this.context.email
      }
    })
  }

  renderSuccessfulOrderCancellationConfirmationModal = (item) => {
    return <ConfirmationModal header="Cancel Request Sent"
                              body={`Cancel request sent for ${item.orderId}. Please allow a few minutes for cancellation to complete.`}
                              show={savedState.isShowSuccessfulOrderCancellationConfirmation}
                              handleExit={() => this.context.redirect({ to: '/transaction' })}
                              handleClose={() => {
                                savedState.isShowSuccessfulOrderCancellationConfirmation = false
                                this.setState({ isShowSuccessfulOrderCancellationConfirmation: savedState.isShowSuccessfulOrderCancellationConfirmation })
                              }}/>
  }

  renderOrderCancelConfirmModal = () => {
    return <ConfirmationModal header={`Are you sure you want to cancel request ${this.props.item.orderId}?`}
                              show={this.state.isShowOrderCancelConfirmModal}
                              handleExit={() => this.context.redirect({ to: '/transaction' })}
                              handleClose={() => this.setState({ isShowOrderCancelConfirmModal: false })}
                              buttonJsx={
                                <>
                                  <Button variant="primary"
                                          disabled={this.state.isDisableOrderCancelConfirmModalButtons}
                                          onClick={async (e) => {
                                            e.preventDefault()
                                            this.setState({
                                              isDisableOrderCancelConfirmModalButtons: true
                                            })
                                            await this.cancelOrder()
                                            savedState.isShowSuccessfulOrderCancellationConfirmation = true
                                            this.setState({
                                              isShowOrderCancelConfirmModal: false,
                                              isShowSuccessfulOrderCancellationConfirmation: savedState.isShowSuccessfulOrderCancellationConfirmation
                                            })
                                          }}>
                                    Yes
                                  </Button>
                                  <Button variant="secondary"
                                          disabled={this.state.isDisableOrderCancelConfirmModalButtons}
                                          onClick={(e) => {
                                            e.preventDefault()
                                            this.setState({ isShowOrderCancelConfirmModal: false })
                                          }}>
                                    No
                                  </Button>
                                </>}/>
  }

  render() {
    const item = this.props.item
    let cancelLink = <></>
    if (item.isCancellable) {
      cancelLink = <a href="" onClick={this.handleOrderCancel}> Cancel</a>
    }

    const userAttributesPopover = (
      <Card body style={{ width: '425px' }}>
      <span className="userAttribute"><span className="userAttributeName">Request ID</span>: {item.orderId}
        {cancelLink}
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
    )

    return (
      <tr>
        <td>{new Date(item.created).toUTCString()}</td>
        <td>
          <OverlayTrigger rootClose={true} trigger="click" placement="right" overlay={userAttributesPopover}>
            <a href="" onClick={(e) => e.preventDefault()}><BsInfoCircleFill/> {item.requester}</a>
          </OverlayTrigger>
          {this.renderOrderCancelConfirmModal()}
          {this.renderSuccessfulOrderCancellationConfirmationModal(item)}
        </td>
        <td>{item.institutionName}</td>
        <td>{item.email}</td>
        <td>{item.filter}</td>
        <td>{Number.parseFloat(item.size / Math.pow(2, 30)).toFixed(2)}GB</td>
        <td>
          <OverlayTrigger
            placement="left"
            overlay={
              <Popover id="popover-basic">
                <Popover.Body>
                  {item.remark}
                </Popover.Body>
              </Popover>
              /* <Tooltip id={`tooltip-status-${item.orderId}`}>
                {item.remark}
              </Tooltip> */
            }>
            <a href="" onClick={(e) => e.preventDefault()}>{item.status}</a>
          </OverlayTrigger>
        </td>
      </tr>)
  }
}

TransactionItem.contextType = AppContext

export default TransactionItem
