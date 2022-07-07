import React, { Component } from 'react'
import './LeftNav.css'
import DimensionAccordion from './DimensionAccordion'

export default class LeftNav extends Component {
  render () {
    return (
      <div className='LeftNav'>
        <DimensionAccordion dimensionData={this.props.dimensionData}
                            onCategorySelect={this.props.onCategorySelect}
                            onCategoryUnselect={this.props.onCategoryUnselect}/>
      </div>
    )
  }
}
