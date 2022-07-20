import BaseHighchartsComponent from './BaseHighchartsComponent'
import { AppContext } from '../lib/context'

const chartOptions = {
  title: {
    text: 'Age'
  }
}

class AgeChart extends BaseHighchartsComponent {
  constructor (props) {
    super(props, { chartOptions, endpoint: '/cerebrum-images/ages?interval=6&max=90&start=12', isNumeric: true, dimension: 'age' })
  }
}

AgeChart.contextType = AppContext

export default AgeChart
