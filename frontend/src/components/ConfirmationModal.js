import React, { Component } from 'react'
import Button from 'react-bootstrap/Button'
import Modal from 'react-bootstrap/Modal'

export default class ConfirmationModal extends Component {
  render() {
    const {
      show,
      handleClose,
      handleExit,
      header,
      body,
      buttonJsx
    } = this.props

    const button = buttonJsx || <Button variant="secondary" onClick={(e) => {
      e.preventDefault()
      handleClose()
    }}>
      Close
    </Button>
    return (
      <Modal show={show} onExit={handleExit}>
        <Modal.Header closeButton>
          <Modal.Title>{header}</Modal.Title>
        </Modal.Header>
        <Modal.Body>{body}</Modal.Body>
        <Modal.Footer>
          {button}
        </Modal.Footer>
      </Modal>
    )
  }
}
