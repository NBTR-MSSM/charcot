import BaseHighchartsComponent from './BaseHighchartsComponent'

const chartOptions = {
  chart: {
    // height: '750px'
  },
  title: {
    text: 'Region'
  }
  /* plotOptions: {
    series: {
      pointWidth: 8
    }
  } */
}

export default class RegionChart extends BaseHighchartsComponent {
  constructor (props) {
    super(props, { chartOptions, endpoint: '/cerebrum-images/regions', dimension: 'region' })
  }
}
