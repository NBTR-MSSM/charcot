import React from 'react'
import ConfirmationModal from '../components/ConfirmationModal'

// FIXME: JMQ: 01/17/2024: Can I leverage 'frontend/src/components/LoaderButton.js' for the <Button/>'s? Check it out
export const orderActionModal = ({ header, body, show, exitHandler, hideHandler, closeHandler, buttonJsx }) => <ConfirmationModal
  header={header}
  body={body}
  show={show}
  handleExit={exitHandler}
  handleHide={hideHandler}
  handleClose={closeHandler}
  buttonJsx={buttonJsx}/>
