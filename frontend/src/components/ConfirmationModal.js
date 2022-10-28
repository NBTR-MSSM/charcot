import React, { Component } from 'react'
import Button from 'react-bootstrap/Button'
import Modal from 'react-bootstrap/Modal'

export default class ConfirmationModal extends Component {
  render () {
    const {
      show,
      handleClose,
      handleExit,
      email
    } = this.props

    return (
      <Modal show={show} onExit={handleExit}>
        <Modal.Header closeButton>
          <Modal.Title>Update Complete</Modal.Title>
        </Modal.Header>
        <Modal.Body>Updates successfully saved for {email}</Modal.Body>
        <Modal.Footer>
          <Button variant="secondary" onClick={(e) => {
            e.preventDefault()
            handleClose()
          }}>
            Close
          </Button>
        </Modal.Footer>
      </Modal>
    )
  }
}
