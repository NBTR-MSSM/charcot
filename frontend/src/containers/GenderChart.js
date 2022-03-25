import BaseHighchartsComponent from './BaseHighchartsComponent'

const chartOptions = {
  chart: {
    type: 'bar'
  },
  title: {
    text: 'Gender'
  }
}

export default class GenderChart extends BaseHighchartsComponent {
  constructor (props) {
    super(props, { chartOptions, endpoint: '/cerebrum-images/sexs', dimension: 'sex' })
  }
}
