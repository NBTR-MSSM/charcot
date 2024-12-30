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
        Your request has been submitted for approval.
      </h3>
      <p>
        <strong>Once your request is approved, it will take 20-30 minutes to assemble the set of files. Depending on the number of files selected, your request might
        be processed in several sets of files, with one email per file set. Do make sure to check your junk/spam folder.</strong>
      </p>
    </div>
  }
}

Confirmation.contextType = AppContext

export default Confirmation
