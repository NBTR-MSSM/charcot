import { Component } from 'react'
import { AppContext } from '../lib/context'

class Confirmation extends Component {
  componentDidMount () {
    this.context.pushToHistory()
    this.context.handleClearFilter()
  }

  render () {
    return <div className='Login'>
      <h3>Your request has been submitted, you'll receive an email when its ready</h3>
    </div>
  }
}

Confirmation.contextType = AppContext

export default Confirmation
