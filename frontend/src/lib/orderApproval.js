import { API, Auth } from 'aws-amplify'
import Button from 'react-bootstrap/Button'
import React from 'react'
import { orderActionModal } from './orderAction'
import Form from 'react-bootstrap/Form'
import LoaderButton from '../components/LoaderButton'

const moreInfoNeeded = async (requesterEmail, orderId, infoNeeded) => {
  await API.patch('charcot', `/cerebrum-image-orders/${orderId}/request-more-info`, {
    headers: {
      Authorization: `Bearer ${(await Auth.currentSession())
        .getAccessToken()
        .getJwtToken()}`
    },
    queryStringParameters: {
      requester: requesterEmail
    },
    body: {
      infoNeeded
    }
  })
  console.log(`JMQ: submit request more info form, infoNeed is ${infoNeeded}`)
}

export const handleMoreInfoNeeded = (event, setStateFunction) => {
  event.preventDefault()
  setStateFunction({
    isShowMoreInfoNeededModal: true
  })
}
const handleMoreInfoNeededSubmit = async (event, componentRef) => {
  event.preventDefault()
  componentRef.setState({
    isDisableMoreInfoNeededModalButtons: true
  })
  await moreInfoNeeded(null, null)
  // persistedState.isShowSuccessfulMoreInfoNeeded = true
  componentRef.setState({
    isShowMoreInfoNeededModal: false,
    isShowSuccessfulMoreInfoNeeded: true
  })
}

export const moreInfoNeededModal = (componentRef) => orderActionModal({
  header: 'Request more info',
  body: <Form onSubmit={handleMoreInfoNeededSubmit} ref={componentRef.moreInfoNeededFormRef}>
    <Form.Group controlId="infoNeeded" size="sm">
      <Form.Label>Please enter the info needed from requester (no more than 500 words)</Form.Label>
      <Form.Control as="textarea"
                    rows={5}
                    value={componentRef.state.infoNeeded}
                    onChange={(event) => {
                      const {
                        id: name,
                        value
                      } = event.target
                      componentRef.setState({ [name]: value })
                    }
                    }/>
    </Form.Group>
  </Form>,
  buttonJsx:
    <>
      <LoaderButton
        block="true"
        size="lg"
        type="submit"
        variant="success"
        onClick={() => componentRef.moreInfoNeededFormRef.current.dispatchEvent(new Event('submit', {
          cancelable: true,
          bubbles: true
        }))}
        isLoading={componentRef.state.isProcessing}
        disabled={!componentRef.state.infoNeeded.length > 0}>
        Submit
      </LoaderButton>
      <Button className="cancel" variant="secondary" size="lg"
              onClick={(e) => {
                e.preventDefault()
                componentRef.setState({ isShowMoreInfoNeededModal: false })
              }}>
        Go Back
      </Button></>,
  hideHandler: () => componentRef.setState({ isShowMoreInfoNeededModal: false }),
  show: componentRef.state.isShowMoreInfoNeededModal

})

export const successfulMoreInfoNeededModal = (item, componentRef) => orderActionModal({
  header: 'Request For More Info Sent',
  body: `You're request for more info has been sent to ${item.requester}`,
  // show: persistedState.isShowSuccessfulMoreInfoNeeded,
  show: componentRef.state.isShowSuccessfulMoreInfoNeeded,
  exitHandler: () => componentRef.context.redirect({ to: '/transaction?reload=true' }),
  closeHandler: () => {
    // persistedState.isShowSuccessfulMoreInfoNeeded = false
    componentRef.setState({ isShowSuccessfulMoreInfoNeeded: false })
  }
})

const approveOrder = async (requesterEmail, orderId) => {
  await API.patch('charcot', `/cerebrum-image-orders/${orderId}/approve`, {
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

export const handleOrderApproval = (event, setStateFunction) => {
  event.preventDefault()
  setStateFunction({
    isShowOrderApprovalConfirmationModal: true
  })
}

export const handleOrderApprovalSubmit = async (event, componentRef, requesterEmail, orderId) => {
  event.preventDefault()
  componentRef.setState({
    isDisableOrderApprovalConfirmationModalButtons: true
  })
  await approveOrder(requesterEmail, orderId)
  // persistedState.isShowSuccessfulOrderApprovalConfirmation = true
  componentRef.setState({
    isShowOrderApprovalConfirmationModal: false,
    isShowSuccessfulOrderApprovalConfirmation: true
  })
}

export const orderApprovalConfirmationModal = (componentRef, requesterEmail, orderId) => orderActionModal({
  header: 'Are you sure you want to approve this request?',
  hideHandler: () => componentRef.setState({ isShowOrderApprovalConfirmationModal: false }),
  show: componentRef.state.isShowOrderApprovalConfirmationModal,
  buttonJsx: <>
    <Button variant="primary"
            disabled={componentRef.state.isDisableOrderApprovalConfirmationModalButtons}
            onClick={async (e) => await handleOrderApprovalSubmit(e, componentRef, requesterEmail, orderId)}>
      Yes
    </Button>
    <Button variant="secondary"
            disabled={componentRef.state.isDisableOrderApprovalConfirmationModalButtons}
            onClick={(e) => {
              e.preventDefault()
              componentRef.setState({ isShowOrderApprovalConfirmationModal: false })
            }}>
      No
    </Button>
  </>
})

export const successfulOrderApprovalConfirmationModal = (item, componentRef) => orderActionModal({
  header: 'Request Approved',
  body: `Request ${item.orderId} has been approved. Processing will begin shortly.`,
  // show: persistedState.isShowSuccessfulOrderApprovalConfirmation,
  show: componentRef.state.isShowSuccessfulOrderApprovalConfirmation,
  exitHandler: () => componentRef.context.redirect({ to: '/transaction?reload=true' }),
  closeHandler: () => {
    // persistedState.isShowSuccessfulOrderApprovalConfirmation = false
    componentRef.setState({ isShowSuccessfulOrderApprovalConfirmation: false })
  }
})
