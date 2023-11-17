import React, { Component } from 'react'
import Button from 'react-bootstrap/Button'
import './FilterComponent.css'
import { AppContext } from '../lib/context'
import featureFlagService from '../lib/FeatureFlagService'

class FilterComponent extends Component {
  resetButtonBackgroundColor = React.createRef()
  handlePredicateButtonClick = async (event) => {
    const target = event.target.id
    if (target.startsWith('remove')) {
      const [dimension, category] = event.target.parentElement.name.split('|')
      await this.context.handleCategoryUnselect({
        dimension,
        category
      })
    } else if (target.startsWith('toggle')) {
      const [dimension] = event.target.parentElement.name.split('|')
      this.context.filter.toggleCategoryLogicalOperator({ dimension })
      const logicalOperator = this.context.filter.categoryLogicalOperator({ dimension })
      for (const category of this.context.filter.categories({ dimension })) {
        document.getElementById(`toggle-${dimension}-${category}`).parentElement.style.backgroundColor = logicalOperator === 'OR' ? '#46d246' : '#ffa500'
      }
      await this.context.handleChartDataStateUpdate({})
    }
  }

  componentDidUpdate() {
    if (this.resetButtonBackgroundColor.current) {
      // This logic clears the background color overwriting set in place when logical operator was toggled to AND,
      // in scenarios where we're dropping back to less than 2 categories. This logic is here because all the elements
      // need to be fully rendered before attempting to access them.
      const {
        dimension,
        category
      } = this.resetButtonBackgroundColor.current
      document.getElementById(`remove-${dimension}-${category}`).parentElement.style = undefined
      this.resetButtonBackgroundColor.current = undefined
    }
  }

  renderCategoryPredicateButton = ({
    dimension,
    category
  }) => {
    const logicalOp = this.context.filter.categoryLogicalOperator({ dimension: dimension })
    const clearPredicateButtonColorClassName = logicalOp === 'OR' ? 'clear-predicate-btn-color-or' : 'clear-predicate-btn-color-and'
    let toggleFragment = ''
    if (featureFlagService.isEnabled('REACT_APP_FEAT_TOGGLE_LOGICAL_OP_ENABLED') && ['region', 'stain'].includes(dimension) && Array.from(this.context.filter.categories({ dimension })).length > 1) {
      toggleFragment = <span className="category-logical-operator-toggle" id={`toggle-${dimension}-${category}`}>
            Toggle to {logicalOp === 'AND' ? 'OR' : 'AND'}
          </span>
    } else {
      this.resetButtonBackgroundColor.current = {
        dimension,
        category
      }
    }
    return <Button key={`key-${category}`} name={`${dimension}|${category}`}
                   className={`clear-predicate-btn ${clearPredicateButtonColorClassName}`}
                   value={category}
                   onClick={this.handlePredicateButtonClick}>{`${dimension}=${category}`}
      <span className="remove" id={`remove-${dimension}-${category}`}>REMOVE</span>
      {toggleFragment}
    </Button>
  }

  render = () => {
    return (
      <div className="FilterComponent">
        <Button id="clear-all-btn" type="reset" onClick={this.context.handleClearFilter}>{'CLEAR ALL'}</Button>
        {this.context.filter.jsx().map(e => this.renderCategoryPredicateButton({
          dimension: e.dimension,
          category: e.category
        }))}
      </div>
    )
  }
}

FilterComponent.contextType = AppContext
export default FilterComponent
