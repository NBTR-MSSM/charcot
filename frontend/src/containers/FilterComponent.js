import React, { Component } from 'react'
import Button from 'react-bootstrap/Button'
import './FilterComponent.css'

export default class FilterComponent extends Component {
  handlePredicateRemove = (event) => {
    const [dimension, category] = event.target.name.split('|')
    this.props.onCategoryUnselect({ dimension, category })
  }

  render = () => {
    return (
      <div className='FilterComponent'>
        <Button id='clear-all-btn' type='reset' onClick={this.props.onClearFilter}>{'CLEAR ALL'}</Button>
        {this.props.filter.jsx().map(e => <Button key={`key-${e.category}`} name={`${e.dimension}|${e.category}`} className='clear-predicate-btn'
                                                      value={e.category}
                                                      onClick={this.handlePredicateRemove}>{`${e.dimension}=${e.category}`}
          <span className='remove'>X</span></Button>)}
      </div>
    )
  }
}
