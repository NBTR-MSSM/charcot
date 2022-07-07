import React, { Component } from 'react'
import Button from 'react-bootstrap/Button'
import './Filter.css'

export default class FilterComponent extends Component {
  render = () => {
    return (
      <div className='Filter'>
        <Button id='clear-all-btn' onClick={this.props.onClearFilter}>{'CLEAR ALL'}</Button>
      </div>
    )
  }
}
