import React, { Component } from 'react'
import { LinkContainer } from 'react-router-bootstrap'
import './Footer.css'
import { API, Auth } from 'aws-amplify'
import Stack from 'react-bootstrap/Stack'
import Stat from './Stat'
import LoaderButton from '../components/LoaderButton'
import { AppContext } from '../lib/context'
import Button from 'react-bootstrap/Button'
import Form from 'react-bootstrap/Form'
import ConfirmationModal from '../components/ConfirmationModal'

const intendedUseForm = (componentRef) => <ConfirmationModal
  header="Enter a valid request reason"
  body={<Form onSubmit={componentRef.handleIntendedUseSubmit} ref={componentRef.intendedUseFormRef}>
    <Form.Group controlId="intendedUse" size="sm">
      <Form.Label>Please enter a brief description of the intended use of images (no more than 500 words)</Form.Label>
      <Form.Control as="textarea"
                    rows={5}
                    value={componentRef.state.intendedUse}
                    onChange={(event) => {
                      const {
                        id: name,
                        value
                      } = event.target
                      componentRef.setState({ [name]: value })
                    }
                    }/>
    </Form.Group>
  </Form>}
  buttonJsx={<>
    <LoaderButton
      block="true"
      size="lg"
      type="submit"
      variant="success"
      onClick={() => componentRef.intendedUseFormRef.current.dispatchEvent(new Event('submit', {
        cancelable: true,
        bubbles: true
      }))}
      isLoading={componentRef.state.isProcessing}
      disabled={!componentRef.state.intendedUse.length > 0}>
      Submit
    </LoaderButton>
    <Button className="cancel" variant="secondary" size="lg"
            onClick={(e) => {
              e.preventDefault()
              componentRef.setState({ isShowRequestReasonForm: false })
            }}>
      Go Back
    </Button></>}
  handleHide={() => componentRef.setState({ isShowRequestReasonForm: false })
  }
  show={componentRef.state.isShowRequestReasonForm}/>

class Footer extends Component {
  constructor(props) {
    super(props)
    this.intendedUseFormRef = React.createRef()
    this.state = {
      isProcessing: false,
      isShowRequestReasonForm: false,
      intendedUse: ''
    }
  }

  handleIntendedUseSubmit = async () => {
    this.setState({ isProcessing: true })
    const filter = this.props.filter.serialize({ isSubmission: true })
    await API.post('charcot', '/cerebrum-image-orders', {
      headers: {
        Authorization: `Bearer ${(await Auth.currentSession())
          .getAccessToken()
          .getJwtToken()}`
      },
      body: {
        filter,
        email: this.context.email,
        intendedUse: this.state.intendedUse
      }
    })
    this.setState({ isProcessing: false })
    this.context.redirect({ to: '/confirmation' })
  }

  handleSubmitButtonClick = async () => {
    if (!this.context.isAuthenticated) {
      this.context.redirect({ to: '/login' })
    } else {
      this.setState({ isShowRequestReasonForm: true })
    }
  }

  render() {
    const buttonInfo = {
      text: 'Next',
      to: '/review',
      id: 'next-btn',
      function: () => {
        console.log('')
      }
    }

    if (this.context.currentPage() === '/review') {
      buttonInfo.text = 'Submit'
      buttonInfo.id = 'submit-btn'
      buttonInfo.function = this.handleSubmitButtonClick
    }

    const backButton = <LinkContainer to="/search">
      <Button id="back-btn">Back</Button>
    </LinkContainer>

    const isProcessing = this.state.isProcessing
    const dimensionData = this.context.dimensionData
    return (
      <footer className="Footer fixed-bottom">
        {intendedUseForm(this)}
        <Stack bsPrefix={'charcot-footer-hstack'} direction="horizontal" gap={3}>
          {Object.values(dimensionData.dimensions).map((e, index) => {
            return <Stat key={index} info={e}/>
          })}
          <Stat
            info={{
              selectedCategoryCount: dimensionData.selectedSlideCount,
              displayName: 'Total Selected Slides'
            }}/>
          <LinkContainer to={buttonInfo.to}>
            <LoaderButton id={buttonInfo.id} onClick={isProcessing ? null : buttonInfo.function}
                          disabled={isProcessing || !this.context.isResultsFound}
                          isLoading={isProcessing}>{isProcessing ? 'Processing...' : buttonInfo.text}
            </LoaderButton>
          </LinkContainer>
          {this.context.currentPage() === '/review' ? backButton : ''}
        </Stack>
      </footer>)
  }
}

Footer.contextType = AppContext

export default Footer
