import { Component } from 'react'
import AgeChart from './AgeChart'
import GenderChart from './GenderChart'
import RegionChart from './RegionChart'
import StainChart from './StainChart'
import RaceChart from './RaceChart'
import DiagnosisChart from './DiagnosisChart'
import './Search.css'
import FilterComponent from './FilterComponent'

export default class Search extends Component {
  componentDidMount () {
    this.props.onRouteLoad({
      active: 'search'
    })
  }

  render () {
    let filterComponent = ''
    if (!this.props.filter.isEmpty()) {
      filterComponent = <FilterComponent onClearFilter={this.props.onClearFilter} filter={this.props.filter}
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
                        onCategoryUnselect={this.props.onCategoryUnselect}
                        dimensionData={this.props.dimensionData}/>
            </td>
            <td>
              <DiagnosisChart filter={this.props.filter}
                              onCategorySelect={this.props.onCategorySelect}
                              onCategoryUnselect={this.props.onCategoryUnselect}
                              dimensionData={this.props.dimensionData}/>
            </td>
          </tr>
          <tr>
            <td>
              <GenderChart filter={this.props.filter}
                           onCategorySelect={this.props.onCategorySelect}
                           onCategoryUnselect={this.props.onCategoryUnselect}
                           dimensionData={this.props.dimensionData}/>
            </td>
            <td>
              <RegionChart filter={this.props.filter}
                           onCategorySelect={this.props.onCategorySelect}
                           onCategoryUnselect={this.props.onCategoryUnselect}
                           dimensionData={this.props.dimensionData}/>
            </td>
          </tr>
          <tr>
            <td><RaceChart filter={this.props.filter}
                           onCategorySelect={this.props.onCategorySelect}
                           onCategoryUnselect={this.props.onCategoryUnselect}
                           dimensionData={this.props.dimensionData}/></td>
            <td>
              <StainChart filter={this.props.filter}
                          onCategorySelect={this.props.onCategorySelect}
                          onCategoryUnselect={this.props.onCategoryUnselect}
                          dimensionData={this.props.dimensionData}/>
            </td>
          </tr>
          </tbody>
        </table>
      </div>)
  }
}
