import { Component } from 'react'
import AgeChart from './AgeChart'
import SexChart from './SexChart'
import RegionChart from './RegionChart'
import StainChart from './StainChart'
import RaceChart from './RaceChart'
import DiagnosisChart from './DiagnosisChart'
import './Search.css'
import FilterComponent from '../components/FilterComponent'
import { AppContext } from '../lib/context'

class Search extends Component {
  componentDidMount () {
    this.context.pushToHistory()
  }

  /**
   * TODO: Convert to bootstrap <Table>, https://react-bootstrap.github.io/components/table/
   */
  render () {
    let filterComponent = ''
    if (!this.props.filter.isEmpty()) {
      filterComponent = <FilterComponent filter={this.props.filter}/>
    }

    return (
      <div className='Search'>
        <h3>Data Search</h3>
        {filterComponent}
        <table>
          <tbody>
          <tr>
            <td>
              <AgeChart filter={this.props.filter}/>
            </td>
            <td>
              <DiagnosisChart filter={this.props.filter}/>
            </td>
          </tr>
          <tr>
            <td>
              <SexChart filter={this.props.filter}/>
            </td>
            <td>
              <RegionChart filter={this.props.filter}/>
            </td>
          </tr>
          <tr>
            <td><RaceChart filter={this.props.filter}/></td>
            <td>
              <StainChart filter={this.props.filter}/>
            </td>
          </tr>
          </tbody>
        </table>
      </div>)
  }
}

Search.contextType = AppContext

export default Search
