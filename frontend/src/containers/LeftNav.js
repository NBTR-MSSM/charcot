import React, { Component } from 'react'
import './LeftNav.css'
import DimensionAccordion from './DimensionAccordion'

export default class LeftNav extends Component {
  render () {
    return (
      <div className='LeftNav'>
        <DimensionAccordion autoExpandActiveDimensions={true}/>
      </div>
    )
  }
}
