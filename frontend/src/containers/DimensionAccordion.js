import React, { Component } from 'react'
import Accordion from 'react-bootstrap/Accordion'
import DimensionAccordionItem from './DimensionAccordionItem'

export default class DimensionAccordion extends Component {
  render () {
    return (
      <Accordion className='DimensionAccordion'>
        {Object.values(this.props.dimensionData).map((e, index) => {
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
