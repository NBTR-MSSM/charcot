import { Component } from 'react'
import AgeChart from './AgeChart'
import GenderChart from './GenderChart'
import RegionChart from './RegionChart'
import StainChart from './StainChart'
import RaceChart from './RaceChart'
import './Search.css'

export default class Search extends Component {
  constructor (props) {
    super(props)
    this.state = {
      filter: {}
    }
  }

  filterClone = () => {
    const clone = {}
    for (const tup of Object.entries(this.state.filter)) {
      const newSet = new Set()
      tup[1].forEach((val) => newSet.add(val))
      clone[tup[0]] = newSet
    }
    return clone
  }

  /**
   * Adds the selected category/value to the filter and
   * refreshes the state
   */
  handleSelect = ({ dimension, category }) => {
    const filter = this.filterClone()
    let values = filter[dimension]
    if (!values) {
      values = new Set()
      filter[dimension] = values
    }
    values.add(category)
    console.log(`JMQ: handleSelect filter is ${Object.entries(filter).map(tup => [tup[0], ...tup[1]])}`)
    this.setState({
      filter: filter,
      updatedDimension: dimension
    })
  }

  /**
   * Does the opposite of 'Search.handleSelect'
   * and refreshes the state.
   */
  handleUnselect = ({ dimension, category }) => {
    const filter = this.filterClone()
    filter[dimension].delete(category)

    // Delete this dimension from the object if
    // the are no more categories selected
    if (!filter[dimension].size) {
      delete filter[dimension]
    }

    console.log(`JMQ: handleUnselect filter is ${Object.entries(filter).map(tup => [tup[0], ...tup[1]])}`)
    this.setState({
      filter: filter,
      updatedDimension: dimension
    })
  }

  render () {
    console.log('JMQ: rendering Search')
    return (
      <div className='Search'>
        <table>
          <tbody>
          <tr>
            <td>
              <AgeChart updatedDimension={this.state.updatedDimension} filter={this.state.filter}
                        onSelect={this.handleSelect}
                        onUnselect={this.handleUnselect}/>
            </td>
            <td>
              <GenderChart updatedDimension={this.state.updatedDimension} filter={this.state.filter}
                           onSelect={this.handleSelect} onUnselect={this.handleUnselect}/>
            </td>
          </tr>
          <tr>
            <td>
              <RegionChart updatedDimension={this.state.updatedDimension} filter={this.state.filter}
                           onSelect={this.handleSelect} onUnselect={this.handleUnselect}/>
            </td>
            <td>
              <StainChart updatedDimension={this.state.updatedDimension} filter={this.state.filter}
                          onSelect={this.handleSelect} onUnselect={this.handleUnselect}/>
            </td>
          </tr>
          <tr>
            <td><RaceChart updatedDimension={this.state.updatedDimension} filter={this.state.filter}
                           onSelect={this.handleSelect} onUnselect={this.handleUnselect}/></td>
            <td></td>
          </tr>
          </tbody>
        </table>
      </div>)
  }
}
