import React, { Component } from 'react'
import { LinkContainer } from 'react-router-bootstrap'
import './Footer.css'
import { API } from 'aws-amplify'
import Stack from 'react-bootstrap/Stack'
import Stat from './Stat'
import LoaderButton from '../components/LoaderButton'
import { AppContext } from '../lib/context'

class Footer extends Component {
  constructor (props) {
    super(props)
    this.state = {
      isProcessing: false
    }
  }

  handleSubmitButtonClick = async () => {
    if (!this.context.isAuthenticated) {
      console.log(`JMQ: not authed to login ${this.context.redirect}`)
      this.context.redirect({ to: 'login' })
    } else {
      this.setState({ isProcessing: true })
      const filter = this.props.filter.serialize()
      console.log(`JMQ: submit clicked, filter is ${filter}`)
      const res = await API.post('charcot', '/cerebrum-image-orders', {
        body: {
          filter,
          email: 'joquijada2010@gmail.com'
        }
      })
      console.log(`JMQ: res is ${JSON.stringify(res)}`)
      this.setState({ isProcessing: false })
      // TODO Redirect to submission confirmation page
    }
  }

  render () {
    const buttonInfo = {
      text: 'Next',
      to: '/checkout',
      id: 'next-btn',
      function: () => {
        console.log('')
      }
    }

    if (this.props.isCheckout) {
      buttonInfo.text = 'Submit'
      buttonInfo.id = 'submit-btn'
      buttonInfo.function = this.handleSubmitButtonClick
    }

    const isProcessing = this.state.isProcessing
    // console.log(`JMQ: footer dimensionData is ${JSON.stringify(this.props.dimensionData)}`)
    return (
      <footer className='Footer fixed-bottom'>
        <Stack bsPrefix={'charcot-footer-hstack'} direction='horizontal' gap={3}>
          {Object.values(this.props.dimensionData).map((e, index) => {
            return <Stat key={index} info={e}/>
          })}
          <LinkContainer to={buttonInfo.to}>
            <LoaderButton id={buttonInfo.id} onClick={isProcessing ? null : buttonInfo.function}
                          disabled={isProcessing}
                          isLoading={isProcessing}>{isProcessing ? 'Processing...' : buttonInfo.text}
            </LoaderButton>
          </LinkContainer>
        </Stack>
      </footer>)
  }
}

Footer.contextType = AppContext

export default Footer
