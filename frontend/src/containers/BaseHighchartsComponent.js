import { Component } from 'react'
import HighchartsReact from 'highcharts-react-official'
import Highcharts from 'highcharts'
import merge from 'lodash.merge'
import { categoryIsSelected, generateStats, serializeFilter } from '../util'

/**
 * Calculated dynamically
 * 1. yAxis.tickInterval: SO that smaller category values are easier to quite, sort of like the precision of the mesurement
 * 2. chart.height: TO control spacing between bars
 * 3. plotOptions.series.pointWidth based on number of categories?
 */
export default class BaseHighchartsComponent extends Component {
  // TODO: Should I just consolidate all the args into 'props'?
  constructor (props, { chartOptions, endpoint, isNumeric = false, dimension }) {
    super(props)
    this.isNumeric = isNumeric
    this.endpoint = endpoint
    this.dimension = dimension
    this.baseChartOptions = {
      chart: {
        type: 'bar',
        height: '200px'
      },
      legend: {
        enabled: false
      },
      yAxis: {
        title: {
          text: null
        },
        tickInterval: 1000
      },
      plotOptions: {
        series: {
          allowPointSelect: true,
          color: '#cccccc',
          point: {
            events: {
              select: this.handleCategorySelect,
              unselect: this.handleCategoryUnselect
            }
          },
          cursor: 'pointer',
          pointWidth: 20,
          dataLabels: {
            enabled: true
          },
          states: {
            select: {
              color: '#46d246'
            }
          }
        }
      }
    }

    merge(this.baseChartOptions, chartOptions)

    this.state = {
      // To avoid unnecessary update keep all options in the state.
      chartOptions: this.baseChartOptions,
      totalSelected: 0
    }
  }

  handleCategorySelect = (event) => {
    const { category, y: value } = event.target
    console.log(`JMQ: Selected category: ${category}, value: ${value}`)
    this.props.onCategorySelect({ dimension: this.dimension, category })
  }

  handleCategoryUnselect = async (event) => {
    const { category, y: value } = event.target
    console.log(`JMQ: Unselected category: ${category}, value: ${value}`)
    this.props.onCategoryUnselect({ dimension: this.dimension, category })
  }

  /**
   * Invokes endpoint to update the chart based on the filter.
   */
  updateChart = async () => {
    /*
     * From the generated Map of category-to-counts, use the keys as X axis
     * and values as Y axis in the chart.
     */
    const { totalPerCategory } = await generateStats({
      endpoint: this.endpoint,
      filter: this.props.filter,
      dimension: this.dimension,
      isNumeric: this.isNumeric
    })

    // TODO: Mark as selected the points that correspond to the categories in the current filter
    this.setState({
      chartOptions: {
        xAxis: {
          categories: Array.from(totalPerCategory.keys()),
          tickInterval: 1
        },
        series: [
          {
            data: Array.from(totalPerCategory.entries()).map(e => ({
              y: e[1],
              selected: categoryIsSelected({ category: e[0], filter: this.props.filter, dimension: this.dimension })
            }))
          }
        ]
      }
    })
  }

  async componentDidMount () {
    await this.updateChart()
  }

  /**
   * If filter changed, update the charts except for the one
   * that originated the change.
   */
  async componentDidUpdate (prevProps) {
    if (this.props.filter !== prevProps.filter && this.props.updatedDimension !== this.dimension) {
      console.log(`JMQ: componentDidUpdate() filter changed ${serializeFilter(this.props.filter, '')}`)
      await this.updateChart()
    }
  }

  render () {
    const { chartOptions } = this.state
    // console.log(`JMQ: Rendering ${this.dimension}, current filter is ${serializeFilter(this.props.filter, this.dimension)}`)
    return (
      <div>
        <HighchartsReact
          highcharts={Highcharts}
          options={chartOptions}
        />
      </div>
    )
  }
}
