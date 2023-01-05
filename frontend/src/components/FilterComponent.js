import React, { Component } from 'react'
import Button from 'react-bootstrap/Button'
import './FilterComponent.css'
import { AppContext } from '../lib/context'

class FilterComponent extends Component {
  handlePredicateRemove = (event) => {
    const [dimension, category] = event.target.parentElement.name.split('|')
    this.context.handleCategoryUnselect({ dimension, category })
  }

  render = () => {
    return (
      <div className='FilterComponent'>
        <Button id='clear-all-btn' type='reset' onClick={this.context.handleClearFilter}>{'CLEAR ALL'}</Button>
        {this.context.filter.jsx().map(e => <Button key={`key-${e.category}`} name={`${e.dimension}|${e.category}`} className='clear-predicate-btn'
                                                      value={e.category}
                                                      onClick={this.handlePredicateRemove}>{`${e.dimension}=${e.category}`}
          <span className='remove'>X</span></Button>)}
      </div>
    )
  }
}

FilterComponent.contextType = AppContext
export default FilterComponent
