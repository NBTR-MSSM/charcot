import React, { Component } from 'react'
import Button from 'react-bootstrap/Button'
import './Filter.css'

export default class FilterComponent extends Component {
  handlePredicateRemove = (event) => {
    const [dimension, category] = event.target.name.split('|')
    console.log(`JMQ: remove predicate ${dimension}, ${category}`)
    this.props.onCategoryUnselect({ dimension, category })
  }

  render = () => {
    return (
      <div className='Filter'>
        <Button id='clear-all-btn' type='reset' onClick={this.props.onClearFilter}>{'CLEAR ALL'}</Button>
        {this.props.filter.jsx(this).map(e => <Button id={e.category} name={`${e.dimension}|${e.category}`} className='clear-predicate-btn'
                                                      value={e.category}
                                                      onClick={this.handlePredicateRemove}>{`${e.dimension}=${e.category}`}
          <span className='remove'>X</span></Button>)}
      </div>
    )
  }
}
