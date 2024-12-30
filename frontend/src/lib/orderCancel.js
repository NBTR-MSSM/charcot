import { API, Auth } from 'aws-amplify'
import Button from 'react-bootstrap/Button'
import React from 'react'
import { orderActionModal } from './orderAction'

const cancelOrder = async (requesterEmail, orderId) => {
  await API.patch('charcot', `/cerebrum-image-orders/${orderId}/cancel`, {
    headers: {
      Authorization: `Bearer ${(await Auth.currentSession())
        .getAccessToken()
        .getJwtToken()}`
    },
    queryStringParameters: {
      requester: requesterEmail
    }
  })
}
export const orderCancelConfirmationModal = (componentRef, persistedState, requesterEmail, orderId) => orderActionModal({
  header: `Are you sure you want to cancel request ${orderId}?`,
  hideHandler: () => componentRef.setState({ isShowOrderCancelConfirmationModal: false }),
  show: componentRef.state.isShowOrderCancelConfirmationModal,
  buttonJsx: <>
    <Button variant="primary"
            disabled={componentRef.state.isDisableOrderCancelConfirmationModalButtons}
            onClick={async (e) => {
              e.preventDefault()
              componentRef.setState({
                isDisableOrderCancelConfirmationModalButtons: true
              })
              await cancelOrder(requesterEmail, orderId)
              persistedState.isShowSuccessfulOrderCancellationConfirmation = true
              componentRef.setState({
                isShowOrderCancelConfirmationModal: false,
                isShowSuccessfulOrderCancellationConfirmation: persistedState.isShowSuccessfulOrderCancellationConfirmation
              })
            }}>
      Yes
    </Button>
    <Button variant="secondary"
            disabled={componentRef.state.isDisableOrderCancelConfirmationModalButtons}
            onClick={(e) => {
              e.preventDefault()
              componentRef.setState({ isShowOrderCancelConfirmationModal: false })
            }}>
      No
    </Button>
  </>
})

export const successfulOrderCancellationConfirmationModal = (item, componentRef) => orderActionModal({
  header: 'Cancel Request Sent',
  body: `Cancel request sent for ${item.orderId}. Please allow a few minutes for cancellation to complete.`,
  // show: persistedState.isShowSuccessfulOrderCancellationConfirmation,
  show: componentRef.state.isShowSuccessfulOrderCancellationConfirmation,
  exitHandler: () => componentRef.context.redirect({ to: '/transaction?reload=true' }),
  closeHandler: () => {
    // persistedState.isShowSuccessfulOrderCancellationConfirmation = false
    componentRef.setState({ isShowSuccessfulOrderCancellationConfirmation: false })
  }
})

export const handleOrderCancel = (event, setStateFunction) => {
  event.preventDefault()
  setStateFunction({
    isShowOrderCancelConfirmationModal: true
  })
}
