import { Component } from 'react'
import HighchartsReact from 'highcharts-react-official'
import Highcharts from 'highcharts'
import merge from 'lodash.merge'

/**
 * TODO: Calculated dynamically
 * 1. yAxis.tickInterval: So that smaller category values are easier to quite, sort of like the precision of the measurement
 * 2. chart.height: To control spacing between bars
 * 3. plotOptions.series.pointWidth based on number of categories?
 */
export default class BaseHighchartsComponent extends Component {
  // TODO: Should I just consolidate all the args into 'props'?
  constructor (props, { chartOptions, dimension }) {
    super(props)
    this.dimension = dimension
    this.baseChartOptions = {
      chart: {
        type: 'bar'
      },
      legend: {
        enabled: false
      },
      yAxis: {
        title: {
          text: null
        }
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

  handleCategoryUnselect = (event) => {
    const { category, y: value } = event.target
    console.log(`JMQ: Unselected category: ${category}, value: ${value}`)
    this.props.onCategoryUnselect({ dimension: this.dimension, category })
  }

  /**
   * Updates this chart and pre-selects categories based
   * on the latest filter.
   */
  updateChart = () => {
    console.log(`JMQ: updateChart ${JSON.stringify(this.props.dimensionData)}`)

    if (Object.keys(this.props.dimensionData).length < 1) {
      return
    }

    const { categories, chartHeight, tickInterval } = this.props.dimensionData[this.dimension]

    this.setState({
      chartOptions: {
        chart: {
          height: chartHeight
        },
        xAxis: {
          categories: Array.from(categories.keys()),
          tickInterval: 1
        },
        yAxis: {
          tickInterval
        },
        series: [
          {
            data: Array.from(categories.entries()).map(e => ({
              y: e[1].count,
              selected: e[1].selected
            }))
          }
        ]
      }
    })
  }

  componentDidMount () {
    console.log(`JMQ: chart ${this.dimension} mounted`)
  }

  /**
   * If filter changed, update the charts except for the one
   * that originated the change.
   */
  componentDidUpdate (prevProps) {
    if (this.props.filter !== prevProps.filter) {
      console.log(`JMQ: ${this.dimension} componentDidUpdate() filter changed prev filter = ${prevProps.filter.serialize()}, current filter = ${this.props.filter.serialize()}`)
      this.updateChart()
    } else {
      console.log(`JMQ: ${this.dimension} componentDidUpdate() did NOT update`)
    }
  }

  render () {
    const { chartOptions } = this.state
    // console.log(`JMQ: Rendering ${this.dimension}, current filter is ${this.props.filter.serialize()}`)
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
