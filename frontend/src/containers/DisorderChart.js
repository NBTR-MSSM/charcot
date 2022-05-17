import BaseHighchartsComponent from './BaseHighchartsComponent'

const chartOptions = {
  title: {
    text: 'Diagnosis'
  }
}

export default class DisorderChart extends BaseHighchartsComponent {
  constructor (props) {
    super(props, { chartOptions, endpoint: '/cerebrum-images/disorders', dimension: 'disorder' })
  }
}
