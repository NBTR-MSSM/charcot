import BaseHighchartsComponent from './BaseHighchartsComponent'
import { AppContext } from '../lib/context'

const chartOptions = {
  title: {
    text: 'Diagnosis'
  }
}

class DiagnosisChart extends BaseHighchartsComponent {
  constructor (props) {
    super(props, { chartOptions, endpoint: '/cerebrum-images/diagnoses', dimension: 'diagnosis' })
  }
}

DiagnosisChart.contextType = AppContext

export default DiagnosisChart
