import BaseHighchartsComponent from './BaseHighchartsComponent'

const chartOptions = {
  title: {
    text: 'Race'
  }
}

export default class RaceChart extends BaseHighchartsComponent {
  constructor (props) {
    super(props, { chartOptions, endpoint: '/cerebrum-images/races', dimension: 'race' })
  }
}
