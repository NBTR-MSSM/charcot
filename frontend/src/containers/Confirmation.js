import { Component } from 'react'
import { AppContext } from '../lib/context'

class Confirmation extends Component {
  componentDidMount () {
    this.context.pushToHistory()
    this.context.handleClearFilter()
  }

  render () {
    return <div className="Login">
      <h3>
        Your request has been submitted, you'll receive an email when its ready
      </h3>
      <p>
        <strong>It will take 20-30 minutes to assemble the files. Depending on the number of files selected, your request might
        be processed in several chunks, with one email per chunk. Do make sure to check your junk folder.</strong>
      </p>
    </div>
  }
}

Confirmation.contextType = AppContext

export default Confirmation
