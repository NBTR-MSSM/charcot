import React, { Component } from 'react'
import Container from 'react-bootstrap/Container'
import './Checkout.css'
import DimensionAccordion from './DimensionAccordion'
import Button from 'react-bootstrap/Button'
import { LinkContainer } from 'react-router-bootstrap'

export default class Checkout extends Component {
  componentDidMount () {
    this.props.onRouteLoad({
      active: 'checkout'
    })
  }

  render = () => {
    return (
      <div className='Checkout'>
        <LinkContainer to='/search'>
          <Button id='back-to-search-btn'>{'< Back to Search'}</Button>
        </LinkContainer>
        <Container bsPrefix={'charcot-checkout-container'}>
          <h3>The Data</h3>
          <DimensionAccordion dimensionData={this.props.dimensionData}
                              onCategorySelect={this.props.onCategorySelect}
                              onCategoryUnselect={this.props.onCategoryUnselect}/>
        </Container>
      </div>)
  }
}
