import React, { Component } from 'react'

export default class Stat extends Component {
  render () {
    const info = this.props.info
    return <span className='charcot-footer-stat'><span>{(info && info[info.statToDisplay || 'selectedCategoryCount']) || 0}</span> {info.displayName}</span>
  }
}
