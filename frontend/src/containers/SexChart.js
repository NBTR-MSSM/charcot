import BaseHighchartsComponent from './BaseHighchartsComponent'

const chartOptions = {
  title: {
    text: 'Sex'
  }
}

export default class SexChart extends BaseHighchartsComponent {
  constructor (props) {
    super(props, { chartOptions, dimension: 'sex' })
  }
}
