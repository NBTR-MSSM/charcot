import React, { Component } from 'react'
import Container from 'react-bootstrap/Container'
import './Review.css'
import DimensionAccordion from './DimensionAccordion'
import Button from 'react-bootstrap/Button'
import { LinkContainer } from 'react-router-bootstrap'

export default class Review extends Component {
  componentDidMount () {
    this.props.onRouteLoad({
      active: 'review'
    })
  }

  render = () => {
    return (
      <div className='Review'>
        <h3>Data Review</h3>
        <LinkContainer to='/search'>
          <Button id='back-to-search-btn'>{'< Back to Search'}</Button>
        </LinkContainer>
        <Container bsPrefix={'charcot-review-container'}>
          <DimensionAccordion dimensionData={this.props.dimensionData}
                              onCategorySelect={this.props.onCategorySelect}
                              onCategoryUnselect={this.props.onCategoryUnselect}/>
        </Container>
      </div>)
  }
}
