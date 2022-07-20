import { Component } from 'react'
import AgeChart from './AgeChart'
import SexChart from './SexChart'
import RegionChart from './RegionChart'
import StainChart from './StainChart'
import RaceChart from './RaceChart'
import DiagnosisChart from './DiagnosisChart'
import './Search.css'
import FilterComponent from './FilterComponent'
import { AppContext } from '../lib/context'

class Search extends Component {
  componentDidMount () {
    this.context.pushToHistory()
  }

  render () {
    let filterComponent = ''
    if (!this.props.filter.isEmpty()) {
      filterComponent = <FilterComponent filter={this.props.filter}
                                         onCategoryUnselect={this.props.onCategoryUnselect}/>
    }

    return (
      <div className='Search'>
        <h3>Data Search</h3>
        {filterComponent}
        <table>
          <tbody>
          <tr>
            <td>
              <AgeChart filter={this.props.filter}
                        onCategorySelect={this.props.onCategorySelect}
                        onCategoryUnselect={this.props.onCategoryUnselect}/>
            </td>
            <td>
              <DiagnosisChart filter={this.props.filter}
                              onCategorySelect={this.props.onCategorySelect}
                              onCategoryUnselect={this.props.onCategoryUnselect}/>
            </td>
          </tr>
          <tr>
            <td>
              <SexChart filter={this.props.filter}
                        onCategorySelect={this.props.onCategorySelect}
                        onCategoryUnselect={this.props.onCategoryUnselect}/>
            </td>
            <td>
              <RegionChart filter={this.props.filter}
                           onCategorySelect={this.props.onCategorySelect}
                           onCategoryUnselect={this.props.onCategoryUnselect}/>
            </td>
          </tr>
          <tr>
            <td><RaceChart filter={this.props.filter}
                           onCategorySelect={this.props.onCategorySelect}
                           onCategoryUnselect={this.props.onCategoryUnselect}/></td>
            <td>
              <StainChart filter={this.props.filter}
                          onCategorySelect={this.props.onCategorySelect}
                          onCategoryUnselect={this.props.onCategoryUnselect}/>
            </td>
          </tr>
          </tbody>
        </table>
      </div>)
  }
}

Search.contextType = AppContext

export default Search
