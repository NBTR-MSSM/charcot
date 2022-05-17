import BaseHighchartsComponent from './BaseHighchartsComponent'

const chartOptions = {
  title: {
    text: 'Age'
  }
}

export default class AgeChart extends BaseHighchartsComponent {
  constructor (props) {
    super(props, { chartOptions, endpoint: '/cerebrum-images/ages?interval=6&max=90&start=12', isNumeric: true, dimension: 'age' })
  }
}
