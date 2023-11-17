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
import { OverlayTrigger, Table, Tooltip } from 'react-bootstrap'
import { BsFillQuestionCircleFill } from 'react-icons/bs'
import Button from 'react-bootstrap/Button'

class Search extends Component {
  componentDidMount() {
    this.context.pushToHistory()
  }

  renderTooltip = () => {
    if (!this.context.filter.isFilterHasMultiValueDimension()) {
      return ''
    }

    return (<OverlayTrigger placement="right"
                           overlay={<Tooltip id="button-tooltip-2">Region and Stain are by default OR'ed together. When two or more of these are selected, you can toggle to AND.</Tooltip>}>
      <Button className="charcot-search-tooltip-btn" variant="link"><BsFillQuestionCircleFill/></Button>
    </OverlayTrigger>)
  }

  render() {
    let filterComponent = ''
    if (!this.props.filter.isEmpty()) {
      filterComponent = <FilterComponent/>
    }

    return (
      <div className="Search">
        <h3>Data Search{this.renderTooltip()}</h3>
        {filterComponent}
        <Table bsPrefix="charcot-search-table">
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
        </Table>
      </div>)
  }
}

Search.contextType = AppContext

export default Search
