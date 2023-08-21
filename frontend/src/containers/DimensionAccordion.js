import React, { Component } from 'react'
import Accordion from 'react-bootstrap/Accordion'
import DimensionAccordionItem from '../components/DimensionAccordionItem'
import { AppContext } from '../lib/context'
import isEqual from 'lodash.isequal'
import difference from 'lodash.difference'
import './DimensionAccordion.css'

const previousDimensionsWithSelectedCategories = React.createRef()
class DimensionAccordion extends Component {
  expandedDimensions = React.createRef()
  isManuallyExpandedOrCollapsedDimension = React.createRef()

  constructor(props) {
    super(props)
    this.state = {
      activeDimensions: []
    }
  }

  componentDidUpdate(prevProps, prevState, snapshot) {
    if (!this.props.autoExpandActiveDimensions) {
      return
    }

    if (this.isManuallyExpandedOrCollapsedDimension.current) {
      this.isManuallyExpandedOrCollapsedDimension.current = false
      return
    }

    const dimensionsWithSelectedCategories = this.obtainDimensionsWithSelectedCategories()
    const newlyActiveDimensions = difference(dimensionsWithSelectedCategories, previousDimensionsWithSelectedCategories.current)
    previousDimensionsWithSelectedCategories.current = dimensionsWithSelectedCategories
    this.expandedDimensions.current = [...(this.expandedDimensions.current || []), ...newlyActiveDimensions]
    if (!isEqual(this.expandedDimensions.current, prevState.activeDimensions)) {
      this.updateActiveDimensions(this.expandedDimensions.current)
    }
  }

  obtainDimensionsWithSelectedCategories() {
    return Array.from(new Set([...Object.values(this.context.dimensionData.dimensions)
      .filter(e => e.selectedCategories.size > 0).map(e => e.dimension)]).values())
  }

  updateActiveDimensions(dimensions) {
    if (!isEqual(this.state.activeDimensions, dimensions)) {
      this.setState(
        {
          activeDimensions: dimensions
        }
      )
    }
  }

  handleExpandDimensionsWithSelectedCategories = (e) => {
    e.preventDefault()
    this.isManuallyExpandedOrCollapsedDimension.current = true
    this.expandedDimensions.current = this.obtainDimensionsWithSelectedCategories()
    this.updateActiveDimensions(this.expandedDimensions.current)
  }

  handleCollapseAllDimensions = (e) => {
    e.preventDefault()
    this.isManuallyExpandedOrCollapsedDimension.current = true
    this.expandedDimensions.current = []
    this.updateActiveDimensions(this.expandedDimensions.current)
  }

  handleDimensionSelection = (dimensions) => {
    this.isManuallyExpandedOrCollapsedDimension.current = true
    this.expandedDimensions.current = dimensions
    this.updateActiveDimensions(dimensions)
  }

  render() {
    return (
      <Accordion onSelect={this.handleDimensionSelection}
                 className='DimensionAccordion' activeKey={this.state.activeDimensions} alwaysOpen>
        <span className="control"><a href=""
                                     onClick={this.handleExpandDimensionsWithSelectedCategories}>View Selections</a><a
          href="" onClick={this.handleCollapseAllDimensions}>Collapse All</a></span>
        {Object.values(this.context.dimensionData.dimensions).map((e, index) => {
          if (e.hideInAccordion) {
            return undefined
          }
          return <DimensionAccordionItem key={index}
                                         eventKey={e.dimension}
                                         info={e}
                                         displayCategoryPredicates={this.props.displayCategoryPredicates}/>
        })}
      </Accordion>
    )
  }
}

DimensionAccordion.contextType = AppContext

export default DimensionAccordion
