import React, { Component } from 'react'
import Container from 'react-bootstrap/Container'
import './Review.css'
import DimensionAccordion from './DimensionAccordion'
import Button from 'react-bootstrap/Button'
import { LinkContainer } from 'react-router-bootstrap'
import { AppContext } from '../lib/context'

class Review extends Component {
  componentDidMount () {
    this.context.pushToHistory()
  }

  render = () => {
    return (
      <div className='Review'>
        <h3>Data Review</h3>
        <LinkContainer to='/search'>
          <Button id='back-to-search-btn'>{'< Back to Search'}</Button>
        </LinkContainer>
        <Container bsPrefix={'charcot-review-container'}>
          <DimensionAccordion onCategorySelect={this.props.onCategorySelect}
                              onCategoryUnselect={this.props.onCategoryUnselect}/>
        </Container>
      </div>)
  }
}

Review.contextType = AppContext

export default Review
