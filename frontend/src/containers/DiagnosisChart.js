import BaseHighchartsComponent from './BaseHighchartsComponent'

const chartOptions = {
  title: {
    text: 'Diagnosis'
  }
}

export default class DiagnosisChart extends BaseHighchartsComponent {
  constructor (props) {
    super(props, { chartOptions, endpoint: '/cerebrum-images/diagnoses', dimension: 'diagnosis' })
  }
}
