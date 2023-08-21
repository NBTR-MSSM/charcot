import React, { Component } from 'react'
import HighchartsReact from 'highcharts-react-official'
import Highcharts from 'highcharts'
import merge from 'lodash.merge'
import Button from 'react-bootstrap/Button'

export default class BaseHighchartsComponent extends Component {
  // TODO: Should I just consolidate all the args into 'props'?
  constructor(props, {
    chartOptions,
    dimension
  }) {
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
      expanded: false,
      chartOptions: this.baseChartOptions,
      totalSelected: 0
    }
  }

  handleExpand = () => {
    this.setState({
      expanded: true
    })
    const elem = document.getElementById(`charcot-search-${this.dimension}`)
    const { realHeight } = this.retrieveDimensionData()
    elem.style.height = realHeight
    elem.classList.toggle('charcot-search-div-gradient')
  }

  handleCollapse = () => {
    this.setState({
      expanded: false
    })
    const elem = document.getElementById(`charcot-search-${this.dimension}`)
    elem.style.height = '200px'
    elem.classList.toggle('charcot-search-div-gradient')
  }

  handleCategorySelect = (event) => {
    const { category } = event.target
    this.context.handleCategorySelect({
      dimension: this.dimension,
      category
    })
  }

  handleCategoryUnselect = (event) => {
    const { category } = event.target
    this.context.handleCategoryUnselect({
      dimension: this.dimension,
      category
    })
  }

  retrieveDimensionData = () => {
    const dimensions = this.context.dimensionData.dimensions
    if (dimensions.length < 1) {
      return undefined
    }
    return (dimensions.filter(e => e.dimension === this.dimension))[0]
  }

  /**
   * Updates this chart and pre-selects categories based
   * on the latest filter.
   */
  updateChart = () => {
    const dimensionData = this.retrieveDimensionData()
    if (!dimensionData) {
      return
    }

    const {
      categories,
      chartHeight,
      tickInterval
    } = dimensionData

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
              selected: e[1].selected,
              events: {
                click: e[1].count < 1 ? () => false : undefined
              }
            })),
            tooltip: {
              pointFormat: '{point.key}<span style="color:#46d246"><strong>{point.y}</strong></span>'
            }
          }
        ]
      }
    })
  }

  componentDidMount() {
    console.log('JMQ: componentDidMount()')
  }

  componentDidUpdate(prevProps) {
    // FIXME: This is doing object identity comparison, is this what I really want?
    if (this.props.filter !== prevProps.filter) {
      this.updateChart()
    }
  }

  renderExpandButton = () => {
    const dimensionData = this.retrieveDimensionData()
    if (!dimensionData || !dimensionData.expandable) {
      return
    }
    return <Button onClick={this.handleExpand} className="charcot-search-expand-btn"
                   type="reset" size="sm">Expand</Button>
  }

  renderCollapseButton = () => {
    const dimensionData = this.retrieveDimensionData()
    if (!dimensionData || !dimensionData.expandable) {
      return
    }
    return <Button onClick={this.handleCollapse} className="charcot-search-collapse-btn"
                   type="reset" size="sm">Collapse</Button>
  }

  render() {
    const { chartOptions } = this.state
    const classes = ['charcot-search-div']
    const dimensionData = this.retrieveDimensionData()
    dimensionData && dimensionData.expandable && classes.push('charcot-search-div-gradient')
    return (
      <div id={`charcot-search-${this.dimension}`} className={classes.join(' ')}>
        <HighchartsReact
          highcharts={Highcharts}
          options={chartOptions}
        />
        {this.state.expanded ? this.renderCollapseButton() : this.renderExpandButton()}
      </div>
    )
  }
}
