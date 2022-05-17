import { Component } from 'react'
import AgeChart from './AgeChart'
import GenderChart from './GenderChart'
import RegionChart from './RegionChart'
import StainChart from './StainChart'
import RaceChart from './RaceChart'
import DisorderChart from './DisorderChart'
import './Search.css'

export default class Search extends Component {
  componentDidMount () {
    this.props.onRouteLoad({
      active: 'search'
    })
  }

  render () {
    return (
      <div className='Search'>
        <table>
          <tbody>
          <tr>
            <td>
              <AgeChart updatedDimension={this.props.updatedDimension} filter={this.props.filter}
                        onCategorySelect={this.props.onCategorySelect}
                        onCategoryUnselect={this.props.onCategoryUnselect}/>
            </td>
            <td>
              <GenderChart updatedDimension={this.props.updatedDimension} filter={this.props.filter}
                           onCategorySelect={this.props.onCategorySelect} onCategoryUnselect={this.props.onCategoryUnselect}/>
            </td>
          </tr>
          <tr>
            <td>
              <RegionChart updatedDimension={this.props.updatedDimension} filter={this.props.filter}
                           onCategorySelect={this.props.onCategorySelect} onCategoryUnselect={this.props.onCategoryUnselect}/>
            </td>
            <td>
              <StainChart updatedDimension={this.props.updatedDimension} filter={this.props.filter}
                          onCategorySelect={this.props.onCategorySelect} onCategoryUnselect={this.props.onCategoryUnselect}/>
            </td>
          </tr>
          <tr>
            <td><RaceChart updatedDimension={this.props.updatedDimension} filter={this.props.filter}
                           onCategorySelect={this.props.onCategorySelect} onCategoryUnselect={this.props.onCategoryUnselect}/></td>
            <td><DisorderChart updatedDimension={this.props.updatedDimension} filter={this.props.filter}
                               onCategorySelect={this.props.onCategorySelect} onCategoryUnselect={this.props.onCategoryUnselect}/></td>
          </tr>
          </tbody>
        </table>
      </div>)
  }
}
