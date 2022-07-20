import BaseHighchartsComponent from './BaseHighchartsComponent'
import { AppContext } from '../lib/context'

const chartOptions = {
  chart: {
  },
  title: {
    text: 'Region'
  }
}

class RegionChart extends BaseHighchartsComponent {
  constructor (props) {
    super(props, { chartOptions, endpoint: '/cerebrum-images/regions', dimension: 'region' })
  }
}

RegionChart.contextType = AppContext

export default RegionChart
