import ProfileManagement from './ProfileManagement'
import { API } from 'aws-amplify'
import { onError } from '../lib/error'
import LoaderButton from '../components/LoaderButton'
import ConfirmationModal from '../components/ConfirmationModal'
import React from 'react'
import Button from 'react-bootstrap/Button'

class EditUser extends ProfileManagement {
  constructor(props) {
    super(props)
    this.state = {
      ...this.state,
      isShowPassword: false
    }
  }

  componentDidUpdate() {
    console.log('JMQ: other user is EditUser componentDidUpdate()')
  }

  async componentDidMount() {
    const user = await this.retrieveUserDetails()
    console.log(`JMQ: other user is ${JSON.stringify(user)}`)
    this.setState({
      ...user,
      isShowSuccessfulUpdateConfirmation: false
    })
  }

  handleProfileChangeSubmit = async (event) => {
    event.preventDefault()
    this.setState({
      isLoading: true
    })
    try {
      await API.put('charcot', `/cerebrum-image-users/${this.context.otherUserEmail}`, {
        body: {
          password: this.state.password,
          ...this.userAttributes()
        }
      })
      this.setState({
        isShowSuccessfulUpdateConfirmation: true
      })
    } catch (e) {
      onError(e)
    }
    this.setState({
      isLoading: false
    })
  }

  renderProfileChangeSubmitButton() {
    return (
      <>
        <LoaderButton
          block="true"
          size="lg"
          type="submit"
          variant="success"
          isLoading={this.state.isLoading}
          disabled={!this.validateForm()}>
          Update
        </LoaderButton>
        <Button className="cancel" variant="secondary" size="lg" onClick={() => this.context.redirect({ to: '/transaction' })}>
          Cancel
        </Button>
      </>
    )
  }

  async retrieveUserDetails() {
    return await API.get('charcot', `/cerebrum-image-users/${this.context.otherUserEmail}`, undefined)
  }

  validateForm() {
    if (this.state.isShowPassword) {
      return super.validateForm()
    } else {
      return true
    }
  }

  renderEmailField() {
    return <></>
  }

  renderPasswordFields = () => {
    if (this.state.isShowPassword) {
      return (<>
        {super.renderPasswordFields()}
        <div className="changePassword">
          <a href="" onClick={(e) => {
            e.preventDefault()
            this.setState({ isShowPassword: false })
          }}>I do NOT want to change the password</a>
        </div>
      </>)
    } else {
      return (
        <div className="changePassword">
          <a href="" onClick={(e) => {
            e.preventDefault()
            this.setState({ isShowPassword: true })
          }}>I want to change the password
          </a>
        </div>)
    }
  }

  renderSuccessConfirmationModal = () => {
    return <ConfirmationModal email={this.context.otherUserEmail}
                              show={this.state.isShowSuccessfulUpdateConfirmation}
                              handleExit={() => this.context.redirect({ to: '/transaction' })}
                              handleClose={() => this.setState({ isShowSuccessfulUpdateConfirmation: false })}/>
  }

  render() {
    return <>{this.renderSuccessConfirmationModal()}{super.render()}</>
  }
}

export default EditUser
