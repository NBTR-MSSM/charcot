import BaseHighchartsComponent from './BaseHighchartsComponent'
import { AppContext } from '../lib/context'

const chartOptions = {
  title: {
    text: 'Age'
  }
}

class AgeChart extends BaseHighchartsComponent {
  constructor (props) {
    super(props, { chartOptions, dimension: 'age' })
  }
}

AgeChart.contextType = AppContext

export default AgeChart
