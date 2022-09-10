import React, { Component } from 'react'
import Form from 'react-bootstrap/Form'
import LoaderButton from './LoaderButton'
import { AppContext } from '../lib/context'
import Button from 'react-bootstrap/Button'
import './SubjectNumberFileUpload.css'

// This is to persist state when user navigates
// to other screens
const savedState = {
  isUploaded: false,
  fileName: undefined,
  subjectNumbers: undefined
}

class SubjectNumberFileUpload extends Component {
  constructor (props) {
    super(props)
    this.fileInput = React.createRef()
    this.state = {
      isUploading: false,
      fileName: undefined,
      subjectNumbers: undefined,
      isUploaded: false
    }
  }

  componentDidUpdate () {
    console.log(`JMQ: SubjectNumberFileUpload componentDidUpdate() ${JSON.stringify(this.context.dimensionData.dimensions)}`)
    /*
     * If user cleared the filter elsewhere in the UI, check if we should reset ourselves
     * to reflect that state.
     */
    if (this.state.isUploaded && !(this.context.dimensionData.dimensions.filter(e => e.dimension === 'subjectNumber' && e.selectedCategoryCount > 0).length)) {
      this.resetState()
    }
  }

  resetState = () => {
    savedState.isUploaded = false
    savedState.fileName = undefined
    savedState.subjectNumbers = undefined
    this.setState({
      ...savedState
    })
  }

  handleSubmit = async (event) => {
    event.preventDefault()
    this.setState({
      isUploading: true
    })
    const file = this.fileInput.current.files[0]
    console.log(`JMQ: Selected file - ${file.name}`)
    const data = await this.readFile(file)
    const subNums = data.split(/\n/)
    const subjectNumbers = subNums.map(num => num.trim()).filter(num => num.match(/^\d+$/)).map(num => parseInt(num))
    for (const num of subjectNumbers) {
      await this.context.handleCategorySelect({ dimension: 'subjectNumber', category: num })
    }

    savedState.isUploaded = true
    savedState.subjectNumbers = subjectNumbers
    this.setState({
      isUploading: false,
      ...savedState
    })
  }

  handleInput = (event) => {
    savedState.fileName = event.target.files[0].name
    this.setState({
      fileName: savedState.fileName
    })
  }

  handleClear = async () => {
    for (const num of savedState.subjectNumbers) {
      await this.context.handleCategoryUnselect({ dimension: 'subjectNumber', category: num })
      console.log(`JMQ: handleClear() removed ${num}`)
    }
    this.resetState()
  }

  readFile = (file) => {
    return new Promise((resolve, reject) => {
      // eslint-disable-next-line no-undef
      const reader = new FileReader()
      reader.onload = (e) => {
        resolve(reader.result)
      }
      reader.onerror = (e) => {
        reject(new Error(`Problem reading ${file}`))
      }
      reader.readAsText(file)
    })
  }

  renderUploadForm = () => (
    <Form onSubmit={this.handleSubmit}>
      <Form.Group controlId="subjectNumberFile" className="mb-3">
        <Form.Control type="file" ref={this.fileInput} className="mb-3" onInput={this.handleInput}/>
      </Form.Group>
      <LoaderButton id='file-upload-submit-btn' block='false' size='sm' type="submit"
                    isLoading={this.state.isUploading}
                    disabled={!this.state.fileName}>
        Upload
      </LoaderButton>
    </Form>
  )

  renderClearButton = () => (
    <Button id='clear-file-btn' type='reset' size='sm'
            onClick={this.handleClear}>File: {savedState.fileName} (REMOVE)</Button>
  )

  render () {
    console.log(`JMQ: render() state is ${JSON.stringify(this.state)}`)
    return (
      <div className="SubjectNumberFileUpload">
        {savedState.isUploaded ? this.renderClearButton() : this.renderUploadForm()}
      </div>
    )
  }
}

SubjectNumberFileUpload.contextType = AppContext

export default SubjectNumberFileUpload
