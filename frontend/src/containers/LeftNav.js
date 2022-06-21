import React, { Component } from 'react'
import Accordion from 'react-bootstrap/Accordion'
import './LeftNav.css'
import LeftNavItem from './LeftNavItem'

export default class LeftNav extends Component {
  render () {
    return (
      <Accordion className='LeftNav'>
        {Object.values(this.props.dimensionData).map((e, index) => {
          if (e.hideInAccordion) {
            return undefined
          }
          return <LeftNavItem key={index}
                              eventKey={index}
                              info={e}
                              onCategorySelect={this.props.onCategorySelect}
                              onCategoryUnselect={this.props.onCategoryUnselect}/>
        })}
      </Accordion>
    )
  }
}
