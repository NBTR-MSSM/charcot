import BaseHighchartsComponent from './BaseHighchartsComponent'
import { AppContext } from '../lib/context'

const chartOptions = {
  title: {
    text: 'Sex'
  }
}

class SexChart extends BaseHighchartsComponent {
  constructor (props) {
    super(props, { chartOptions, dimension: 'sex' })
  }
}

SexChart.contextType = AppContext

export default SexChart
