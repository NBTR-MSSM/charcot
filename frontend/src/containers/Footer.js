import React, { Component } from 'react'
import { LinkContainer } from 'react-router-bootstrap'
import './Footer.css'

export default class Footer extends Component {
  render () {
    return (<footer className='Footer fixed-bottom'>
      <LinkContainer to="/checkout">
        <button id='next-btn'>Next</button>
      </LinkContainer>
    </footer>)
  }
}
