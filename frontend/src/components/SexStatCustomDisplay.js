import React, { Component } from 'react'
import { AppContext } from '../lib/context'

class SexStatCustomDisplay extends Component {
  constructor(props) {
    super(props)
    this.bar = 'foo'
  }

  render() {
    const male = this.props.info.categories.get('Male')
    const female = this.props.info.categories.get('Female')
    return <>
      <span
        className="charcot-footer-stat"><span>{(!female.selected && male.count) || (male.selected && male.count) || 0}</span> Male</span>
      <span
        className="charcot-footer-stat"><span>{(!male.selected && female.count) || (female.selected && female.count) || 0}</span> Female</span>
    </>
  }
}

SexStatCustomDisplay.contextType = AppContext
export default SexStatCustomDisplay
