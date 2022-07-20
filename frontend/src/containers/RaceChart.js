import BaseHighchartsComponent from './BaseHighchartsComponent'
import { AppContext } from '../lib/context'

const chartOptions = {
  title: {
    text: 'Race'
  }
}

class RaceChart extends BaseHighchartsComponent {
  constructor (props) {
    super(props, { chartOptions, endpoint: '/cerebrum-images/races', dimension: 'race' })
  }
}
RaceChart.contextType = AppContext

export default RaceChart
