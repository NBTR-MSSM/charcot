import React, { Component } from 'react'
import Accordion from 'react-bootstrap/Accordion'
import DimensionAccordionItem from './DimensionAccordionItem'
import { AppContext } from '../lib/context'

class DimensionAccordion extends Component {
  render () {
    return (
      <Accordion className='DimensionAccordion'>
        {Object.values(this.context.dimensionData.dimensions).map((e, index) => {
          if (e.hideInAccordion) {
            return undefined
          }
          return <DimensionAccordionItem key={index}
                                         eventKey={index}
                                         info={e}
                                         onCategorySelect={this.props.onCategorySelect}
                                         onCategoryUnselect={this.props.onCategoryUnselect}/>
        })}
      </Accordion>
    )
  }
}

DimensionAccordion.contextType = AppContext

export default DimensionAccordion
