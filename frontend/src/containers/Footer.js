import React, { Component } from 'react'
import { LinkContainer } from 'react-router-bootstrap'
import './Footer.css'
import { API, Auth } from 'aws-amplify'
import Stack from 'react-bootstrap/Stack'
import Stat from './Stat'
import LoaderButton from '../components/LoaderButton'
import { AppContext } from '../lib/context'
import Button from 'react-bootstrap/Button'

class Footer extends Component {
  constructor(props) {
    super(props)
    this.state = {
      isProcessing: false
    }
  }

  handleSubmitButtonClick = async () => {
    if (!this.context.isAuthenticated) {
      this.context.redirect({ to: '/login' })
    } else {
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
          email: this.context.email
        }
      })
      this.setState({ isProcessing: false })
      this.context.redirect({ to: '/confirmation' })
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
