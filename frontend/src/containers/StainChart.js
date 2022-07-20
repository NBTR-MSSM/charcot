import BaseHighchartsComponent from './BaseHighchartsComponent'
import { AppContext } from '../lib/context'

const chartOptions = {
  title: {
    text: 'Stain'
  }
}

class StainChart extends BaseHighchartsComponent {
  constructor (props) {
    super(props, { chartOptions, endpoint: '/cerebrum-images/stains', dimension: 'stain' })
  }
}

StainChart.contextType = AppContext

export default StainChart
