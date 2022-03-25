import { Component } from 'react'
import HighchartsReact from 'highcharts-react-official'
import Highcharts from 'highcharts'
import merge from 'lodash.merge'
import { API } from 'aws-amplify'

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
        height: '350px'
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
          color: '#46d246',
          point: {
            events: {
              select: this.handleSelect,
              unselect: this.handleUnselect
            }
          },
          allowPointSelect: true,
          cursor: 'pointer',
          pointWidth: 20,
          dataLabels: {
            enabled: true
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

  handleSelect = (event) => {
    const { category, y: value } = event.target
    console.log(`Category: ${category}, value: ${value}`)
    /* this.setState({
      totalSelected: this.state.totalSelected + value
    }) */
    this.props.onSelect({ dimension: this.dimension, category })
  }

  handleUnselect = async (event) => {
    const { category, y: value } = event.target
    console.log(`Category: ${category}, value: ${value}`)
    /* this.setState({
      totalSelected: this.state.totalSelected - value
    }) */
    this.props.onUnselect({ dimension: this.dimension, category })
    // await this.updateChart()
  }

  /**
   * Invokes endpoint to update the chart based in the filter.
   */
  updateChart = async () => {
    const filter = Object.entries(this.props.filter).filter((tup) => tup[0] !== this.dimension).map(tup => {
      const cats = Array.from(tup[1].values()).map(val => `${tup[0]} = '${val}'`)
      const catsStr = cats.join(' OR ')
      return cats.length > 1 ? `(${catsStr})` : catsStr
    })

    const values = await API.get('charcot', this.endpoint, {
      queryStringParameters: {
        filter: filter.length > 0 ? `${filter.join(' AND ')}` : undefined
      }
    })

    /*
     * Generate a Map of category-to-counts. Use the keys as X axis
     * and values as Y axis.
     */
    const categoryToTotal = values.reduce((prev, cur) => {
      if (this.isNumeric) {
        const key = cur.range
        const cnt = cur.count
        let total
        if (!(total = prev.get(key))) {
          prev.set(key, cnt)
        } else {
          prev.set(key, cnt + total)
        }
      } else {
        prev.set(cur.title, cur.count)
      }

      return prev
    }, new Map())

    this.setState({
      chartOptions: {
        xAxis: {
          categories: Array.from(categoryToTotal.keys()).map(e => e),
          tickInterval: 1
        },
        series: [
          { data: Array.from(categoryToTotal.values()).map(e => e) }
        ]
      }
    })
  }

  async componentDidMount () {
    await this.updateChart()
  }

  /**
   * If filter changed update the other charts other than the one
   * on which the change was affected.
   */
  async componentDidUpdate (prevProps) {
    if (this.props.filter !== prevProps.filter && this.props.updatedDimension !== this.dimension) {
      await this.updateChart()
    }
  }

  render () {
    const { chartOptions } = this.state
    console.log(`JMQ: Rendering ${this.dimension}`)
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
