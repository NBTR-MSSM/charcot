import BaseHighchartsComponent from './BaseHighchartsComponent'

const chartOptions = {
  chart: {
    type: 'bar'
  },
  title: {
    text: 'Race'
  }
}

export default class RaceChart extends BaseHighchartsComponent {
  constructor (props) {
    super(props, { chartOptions, endpoint: '/cerebrum-images/races', dimension: 'race' })
  }
}
