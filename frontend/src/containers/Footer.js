import React, { Component } from 'react'
import { LinkContainer } from 'react-router-bootstrap'
import Button from 'react-bootstrap/Button'
import './Footer.css'
import { API } from 'aws-amplify'
import { serializeFilter } from '../util'
import Stack from 'react-bootstrap/Stack'
import Stat from './Stat'

export default class Footer extends Component {
  constructor (props) {
    super(props)
    this.state = {
      isProcessing: false
    }
  }

  handleSubmitButtonClick = async () => {
    this.setState({ isProcessing: true })
    const filter = serializeFilter(this.props.filter)
    console.log(`JMQ: submit clicked, filter is ${filter}`)
    const res = await API.post('charcot', '/cerebrum-image-orders', {
      body: {
        filter,
        email: 'joquijada2010@gmail.com'
      }
    })
    console.log(`JMQ: res is ${JSON.stringify(res)}`)
    this.setState({ isProcessing: false })
    // TODO Trigger a popup with results
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
            <Button id={buttonInfo.id} onClick={isProcessing ? null : buttonInfo.function}
                    disabled={isProcessing}>{isProcessing ? 'Processing...' : buttonInfo.text}</Button>
          </LinkContainer>
        </Stack>
      </footer>)
  }
}
