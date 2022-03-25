import BaseHighchartsComponent from './BaseHighchartsComponent'

const chartOptions = {
  chart: {
    type: 'bar'
  },
  title: {
    text: 'Stain'
  }
}

export default class StainChart extends BaseHighchartsComponent {
  constructor (props) {
    super(props, { chartOptions, endpoint: '/cerebrum-images/stains', dimension: 'stain' })
  }
}
