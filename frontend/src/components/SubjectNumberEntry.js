import React, { Component } from 'react'
import Form from 'react-bootstrap/Form'
import LoaderButton from './LoaderButton'
import { AppContext } from '../lib/context'
import Button from 'react-bootstrap/Button'
import './SubjectNumberEntry.css'
import { OverlayTrigger, Tooltip } from 'react-bootstrap'

// This is to persist state when user navigates
// to other screens
const persistedState = {
  isFileProcessed: false,
  isManualEntryProcessed: false,
  fileName: undefined,
  subjectNumbers: [],
  isUserPrefersFileUpload: false,
  subjectNumberListEntry: ''
}

class SubjectNumberEntry extends Component {
  constructor(props) {
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

  async componentDidMount() {
    this.resetIfNecessary()
  }

  componentDidUpdate() {
    this.resetIfNecessary()
  }

  resetIfNecessary = () => {
    // If user cleared subnum selections in the UI, or completely cleared the filter, reset ourselves.
    const filter = this.context.filter
    if ((persistedState.isFileProcessed || persistedState.isManualEntryProcessed) &&
      (!filter.has({ dimension: 'subjectNumber' }) || filter.isEmpty())) {
      this.resetState()
    }
  }

  handleEntryModeChange = async (event) => {
    event.preventDefault()
    // toggle between manual and file input each time this method called
    persistedState.isUserPrefersFileUpload = !persistedState.isUserPrefersFileUpload
    const skip = new Set()
    skip.add('subjectNumberListEntry')

    await this.handleClear(undefined, skip)

    this.setState({
      isUserPrefersFileUpload: persistedState.isUserPrefersFileUpload
    })
  }

  handleSubmit = async (event) => {
    event.preventDefault()

    this.setState({
      isProcessing: true
    })

    let subjectNumbers
    if (persistedState.isUserPrefersFileUpload) {
      const file = this.fileInput.current.files[0]
      const data = await this.readFile(file)
      subjectNumbers = data.split(/\n/).map(num => num.trim()).filter(num => num.match(/^\d+$/)).map(num => parseInt(num))
      persistedState.isFileProcessed = true
    } else {
      subjectNumbers = persistedState.subjectNumberListEntry.split(/,/).map(num => num.trim()).map(num => parseInt(num))
      persistedState.isManualEntryProcessed = true
    }

    for (const num of subjectNumbers) {
      await this.context.handleCategorySelect({
        dimension: 'subjectNumber',
        category: num
      })
    }

    persistedState.subjectNumbers = subjectNumbers
    this.setState({
      isProcessing: false,
      ...persistedState
    })
  }

  handleInput = (event) => {
    persistedState.fileName = event.target.files[0].name
    this.setState({
      fileName: persistedState.fileName
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
        persistedState[key] = false
      } else if (key === 'subjectNumberListEntry') {
        persistedState[key] = ''
      } else if (key === 'subjectNumbers') {
        persistedState[key] = []
      } else {
        persistedState[key] = undefined
      }
      newState[key] = persistedState[key]
    }
    /*
     * Make sure we update the state only for the properties
     * that have been modified, hence the reason for using the newState
     * helper object to accomplish this.
     */
    this.setState(newState)
  }

  handleClear = async (event, skip = new Set()) => {
    for (const num of persistedState.subjectNumbers) {
      await this.context.handleCategoryUnselect({
        dimension: 'subjectNumber',
        category: num
      })
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

  validateSubjectNumberListEntry = () => persistedState.subjectNumberListEntry.match(/^(\d+\s*,\s*)*\d+$/)

  handleFormChange = (event) => {
    const {
      id,
      value
    } = event.target
    persistedState[id] = value
    this.setState({
      [id]: persistedState[id]
    })
  }

  renderSubNumFileUploadForm = () => (
    <>
      <OverlayTrigger
        key="right"
        placement="right"
        overlay={
          <Tooltip id="tooltip-file-upload">
            Only text files accepted. The file should contain a <strong>single</strong> subject number per line,
            example:<br/>
            12345<br/>
            67893<br/>
            34<br/>
            99<br/>
          </Tooltip>
        }>
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
    <Form onSubmit={this.handleSubmit}>
      <Form.Group controlId="subjectNumberListEntry" size="lg">
        <Form.Label>Enter list of comma separated subject numbers (Ex: 23,99,754,139,5):</Form.Label>
        <Form.Control as="textarea"
                      rows={5}
                      value={persistedState.subjectNumberListEntry}
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
  )

  renderFileClearButton = () => (
    <>
      <Button id="clear-file-btn" type="reset" size="sm"
              onClick={this.handleClear}>File: {persistedState.fileName} (REMOVE)</Button>
    </>
  )

  renderManualEntryClearButton = () => (
    <>
      <span id="subject-number-list">{persistedState.subjectNumbers.join(', ')}</span>
      <Button id="clear-file-btn" type="reset" size="sm"
              onClick={this.handleClear}>Clear</Button>
    </>
  )

  render() {
    let fragmentToRender
    if (persistedState.isUserPrefersFileUpload) {
      fragmentToRender = persistedState.isFileProcessed ? this.renderFileClearButton() : this.renderSubNumFileUploadForm()
    } else {
      fragmentToRender = persistedState.isManualEntryProcessed ? this.renderManualEntryClearButton() : this.renderSubNumEntryForm()
    }
    return (
      <div className="SubjectNumberEntry">
        <span id="sub-num-entry-mode-decision">
          <a href=""
             onClick={this.handleEntryModeChange}>{persistedState.isUserPrefersFileUpload ? 'I want to enter subject numbers manually instead' : 'I want to upload a file instead'}</a>
        </span>
        {fragmentToRender}
      </div>
    )
  }
}

SubjectNumberEntry.contextType = AppContext

export default SubjectNumberEntry
