import React, { Component } from 'react'
import Form from 'react-bootstrap/Form'
import LoaderButton from './LoaderButton'
import { AppContext } from '../lib/context'
import Button from 'react-bootstrap/Button'
import './SubjectNumberEntry.css'
import { OverlayTrigger, Tooltip } from 'react-bootstrap'

// This is to persist state when user navigates
// to other screens
const savedState = {
  isFileProcessed: false,
  isManualEntryProcessed: false,
  fileName: undefined,
  subjectNumbers: [],
  isUserPrefersFileUpload: false,
  subjectNumberListEntry: ''
}

class SubjectNumberEntry extends Component {
  constructor (props) {
    super(props)
    this.fileInput = React.createRef()
    this.state = {
      isProcessing: false,
      fileName: undefined,
      subjectNumbers: [],
      isFileProcessed: false,
      isManualEntryProcessed: false,
      isUserPrefersFileUpload: false,
      subjectNumberListEntry: ''
    }
  }

  async componentDidMount () {
    // console.log('JMQ: SubjectNumberFileUpload componentDidMount()')
    this.resetIfNecessary()
  }

  componentDidUpdate () {
    this.resetIfNecessary()
  }

  resetIfNecessary = () => {
    // If user cleared subnum selections in the UI, or completely cleared the filter, reset ourselves.
    const filter = this.context.filter
    if ((savedState.isFileProcessed || savedState.isManualEntryProcessed) &&
      (!filter.has({ dimension: 'subjectNumber' }) || filter.isEmpty())) {
      // console.log(`JMQ: resetIfNecessary() filter is ${JSON.stringify(filter.serialize())}, filter empty? ${filter.isEmpty()}, savedState is ${JSON.stringify(savedState)}`)
      this.resetState()
    }
  }

  handleEntryModeChange = async (event) => {
    event.preventDefault()
    savedState.isUserPrefersFileUpload = !savedState.isUserPrefersFileUpload
    const skip = new Set()
    skip.add('subjectNumberListEntry')

    if (savedState.isUserPrefersFileUpload) {
      // switching to subnum file upload
      await this.handleClear(undefined, skip)
    } else {
      // Switching to manual subnum entry
      await this.handleClear(undefined, skip)
    }

    this.setState({
      isUserPrefersFileUpload: savedState.isUserPrefersFileUpload
    })
  }

  handleSubmit = async (event) => {
    event.preventDefault()

    this.setState({
      isProcessing: true
    })

    let subjectNumbers
    if (savedState.isUserPrefersFileUpload) {
      const file = this.fileInput.current.files[0]
      // console.log(`JMQ: Selected file - ${file.name}`)
      const data = await this.readFile(file)
      subjectNumbers = data.split(/\n/).map(num => num.trim()).filter(num => num.match(/^\d+$/)).map(num => parseInt(num))
      savedState.isFileProcessed = true
    } else {
      subjectNumbers = savedState.subjectNumberListEntry.split(/,/).map(num => num.trim()).map(num => parseInt(num))
      savedState.isManualEntryProcessed = true
    }

    for (const num of subjectNumbers) {
      await this.context.handleCategorySelect({
        dimension: 'subjectNumber',
        category: num
      })
    }

    savedState.subjectNumbers = subjectNumbers
    this.setState({
      isProcessing: false,
      ...savedState
    })
  }

  handleInput = (event) => {
    savedState.fileName = event.target.files[0].name
    this.setState({
      fileName: savedState.fileName
    })
  }

  resetState = (skip = new Set()) => {
    // We don't want to reset 'isUserPrefersFileUpload' to its
    // default of 'false'. We want to always remember whatever
    // the user selected last
    skip.add('isUserPrefersFileUpload')
    const newState = {}
    for (const key of Object.keys(this.state)) {
      if (skip.has(key)) {
        continue
      }
      if (key.startsWith('is')) {
        savedState[key] = false
      } else if (key === 'subjectNumberListEntry') {
        savedState[key] = ''
      } else if (key === 'subjectNumbers') {
        savedState[key] = []
      } else {
        savedState[key] = undefined
      }
      newState[key] = savedState[key]
    }
    /*
     * Make sure we update the state only of the properties
     * that have been modified, hence the reason for using the newState
     * helper object to accomplish this.
     */
    // console.log(`JMQ: resetState() newState is ${JSON.stringify(newState)}`)
    this.setState(newState)
  }

  handleClear = async (event, skip = new Set()) => {
    for (const num of savedState.subjectNumbers) {
      await this.context.handleCategoryUnselect({
        dimension: 'subjectNumber',
        category: num
      })
      // console.log(`JMQ: handleClear() removed ${num}`)
    }
    this.resetState(skip)
  }

  readFile = (file) => {
    return new Promise((resolve, reject) => {
      // eslint-disable-next-line no-undef
      const reader = new FileReader()
      reader.onload = () => {
        resolve(reader.result)
      }
      reader.onerror = () => {
        reject(new Error(`Problem reading ${file}`))
      }
      reader.readAsText(file)
    })
  }

  validateSubjectNumberListEntry = () => savedState.subjectNumberListEntry.match(/^(\d+\s*,\s*)*\d+$/)

  handleFormChange = (event) => {
    const {
      id,
      value
    } = event.target
    savedState[id] = value
    this.setState({
      [id]: savedState[id]
    })
  }

  renderSubNumFileUploadForm = () => (
    <>
      <OverlayTrigger
        key="right"
        placement="right"
        overlay={
          <Tooltip id="tooltip-file-upload">
            Only text files accepted. The file should contain a <strong>single</strong> subject number per line, example:<br/>
              12345<br/>
              67893<br/>
              34<br/>
              99<br/>
          </Tooltip>
        }
      >
        <Form onSubmit={this.handleSubmit}>
          <Form.Group controlId="subjectNumberFile" className="mb-3">
            <Form.Control type="file" ref={this.fileInput} className="mb-3" onInput={this.handleInput}/>
          </Form.Group>
          <LoaderButton id="file-upload-submit-btn" block="false" size="sm" type="submit"
                        isLoading={this.state.isProcessing}
                        disabled={!this.state.fileName}>
            Upload
          </LoaderButton>
        </Form>
      </OverlayTrigger>
    </>
  )

  renderSubNumEntryForm = () => (
    <>
      <Form onSubmit={this.handleSubmit}>
        <Form.Group controlId="subjectNumberListEntry" size="lg">
          <Form.Label>Enter list of comma separated subject numbers (Ex: 23,99,754,139,5):</Form.Label>
          <Form.Control as="textarea"
                        rows={5}
                        value={savedState.subjectNumberListEntry}
                        onChange={this.handleFormChange}/>
        </Form.Group>
        <LoaderButton
          block="true"
          size="sm"
          type="submit"
          variant="success"
          isLoading={this.state.isProcessing}
          disabled={!this.validateSubjectNumberListEntry()}>
          Submit
        </LoaderButton>
      </Form>
    </>
  )

  renderFileClearButton = () => (
    <>
      <Button id="clear-file-btn" type="reset" size="sm"
              onClick={this.handleClear}>File: {savedState.fileName} (REMOVE)</Button>
    </>
  )

  renderManualEntryClearButton = () => (
    <>
      <span id="subject-number-list">{savedState.subjectNumbers.join(', ')}</span>
      <Button id="clear-file-btn" type="reset" size="sm"
              onClick={this.handleClear}>Clear</Button>
    </>
  )

  render () {
    // console.log(`JMQ: render() state is ${JSON.stringify(this.state)}`)
    let fragmentToRender
    if (savedState.isUserPrefersFileUpload) {
      fragmentToRender = savedState.isFileProcessed ? this.renderFileClearButton() : this.renderSubNumFileUploadForm()
    } else {
      fragmentToRender = savedState.isManualEntryProcessed ? this.renderManualEntryClearButton() : this.renderSubNumEntryForm()
    }
    return (
      <div className="SubjectNumberEntry">
        <span id="sub-num-entry-mode-decision">
          <a href=""
             onClick={this.handleEntryModeChange}>{savedState.isUserPrefersFileUpload ? 'I want to enter subject numbers manually instead' : 'I want to upload a file instead'}</a>
        </span>
        {fragmentToRender}
      </div>
    )
  }
}

SubjectNumberEntry.contextType = AppContext

export default SubjectNumberEntry
