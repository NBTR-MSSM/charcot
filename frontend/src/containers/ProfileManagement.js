import React, { Component } from 'react'
import Form from 'react-bootstrap/Form'
import LoaderButton from '../components/LoaderButton'
import { AppContext } from '../lib/context'
import { onError } from '../lib/error'
import './ProfileManagement.css'
import { Auth } from 'aws-amplify'

class ProfileManagement extends Component {
  constructor(props) {
    super(props)
    this.state = {
      email: '',
      password: '',
      confirmPassword: '',
      confirmationCode: '',
      firstName: '',
      lastName: '',
      degree: '',
      institutionName: '',
      institutionAddress: '',
      areasOfInterest: '',
      intendedUse: '',
      newUser: '',
      isLoading: false
    }
  }

  componentDidMount() {
    this.context.pushToHistory()
  }

  validateForm() {
    return (
      this.state.email.length > 0 &&
      this.state.password.length > 0 &&
      this.state.password === this.state.confirmPassword
    )
  }

  validateConfirmationForm = () => {
    return this.state.confirmationCode.length > 0
  }

  userAttributes = () => {
    const {
      firstName,
      lastName,
      degree,
      institutionName,
      institutionAddress,
      areasOfInterest,
      intendedUse
    } = this.state
    const obj = {}
    obj.family_name = lastName
    obj.given_name = firstName
    obj['custom:degree'] = degree
    obj['custom:institutionName'] = institutionName
    obj['custom:institutionAddress'] = institutionAddress
    obj['custom:areasOfInterest'] = areasOfInterest
    obj['custom:intendedUse'] = intendedUse
    return obj
  }

  handleProfileChangeSubmit = async () => {
    throw new Error('Must implement handleProfileChangeSubmit!')
  }

  handleConfirmationSubmit = async (event) => {
    event.preventDefault()
    this.setState({
      isLoading: true
    })
    const {
      confirmationCode,
      email,
      password
    } = this.state

    try {
      await Auth.confirmSignUp(email, confirmationCode)
      await Auth.signIn(email, password)
      this.context.handleLogin({ email })
      this.context.redirect({ to: '/home' })
    } catch (e) {
      onError(e)
    }

    this.setState({
      isLoading: false
    })
  }

  /**
   * Children of this class get this method for free, which updates the ReactJS state with
   * values entered into the form. This reduces a lot of boilerplate in profile related containers.
   */
  handleFormChange = (event) => {
    const newState = {}
    const {
      id,
      value
    } = event.target
    newState[id] = value
    this.setState(newState)
  }

  renderConfirmationForm = () => {
    return (
      <Form onSubmit={this.handleConfirmationSubmit}>
        <Form.Group controlId="confirmationCode" size="lg">
          <Form.Label>Confirmation Code</Form.Label>
          <Form.Control
            autoFocus
            type="tel"
            onChange={this.handleFormChange}
            value={this.state.confirmationCode}
          />
          <Form.Text muted>Please check your email for the code.</Form.Text>
        </Form.Group>
        <LoaderButton
          block="true"
          size="lg"
          type="submit"
          variant="success"
          isLoading={this.state.isLoading}
          disabled={!this.validateConfirmationForm()}>
          Verify
        </LoaderButton>
      </Form>
    )
  }

  renderPasswordFields() {
    return (<>
      <Form.Group controlId="password" size="lg">
        <Form.Label>Password</Form.Label>
        <Form.Control
          type="password"
          value={this.state.password}
          onChange={this.handleFormChange}
        />
      </Form.Group>
      <Form.Group controlId="confirmPassword" size="lg">
        <Form.Label>Confirm Password</Form.Label>
        <Form.Control
          type="password"
          onChange={this.handleFormChange}
          value={this.state.confirmPassword}
        />
      </Form.Group>
    </>)
  }

  renderEmailField() {
    return <Form.Group controlId="email" size="lg">
      <Form.Label>Email</Form.Label>
      <Form.Control
        autoFocus
        type="email"
        value={this.state.email}
        onChange={this.handleFormChange}
      />
    </Form.Group>
  }

  renderProfileChangeForm = () => {
    return (
      <Form onSubmit={this.handleProfileChangeSubmit}>
        {this.renderEmailField()}
        {this.renderPasswordFields()}
        <Form.Group controlId="firstName" size="lg">
          <Form.Label>First Name</Form.Label>
          <Form.Control
            type="text"
            value={this.state.firstName}
            onChange={this.handleFormChange}
          />
        </Form.Group>
        <Form.Group controlId="lastName" size="lg">
          <Form.Label>Last Name</Form.Label>
          <Form.Control
            type="text"
            value={this.state.lastName}
            onChange={this.handleFormChange}
          />
        </Form.Group>
        <Form.Group controlId="degree" size="lg">
          <Form.Label>Degree</Form.Label>
          <Form.Control
            type="text"
            value={this.state.degree}
            onChange={this.handleFormChange}
          />
        </Form.Group>
        <Form.Group controlId="institutionName" size="lg">
          <Form.Label>Institution Name</Form.Label>
          <Form.Control
            type="text"
            value={this.state.institutionName}
            onChange={this.handleFormChange}
          />
        </Form.Group>
        <Form.Group controlId="institutionAddress" size="lg">
          <Form.Label>Institution Address</Form.Label>
          <Form.Control as="textarea"
                        rows={3}
                        value={this.state.institutionAddress}
                        onChange={this.handleFormChange}/>
        </Form.Group>
        <Form.Group controlId="areasOfInterest" size="lg">
          <Form.Label>Areas of Scientific Interest</Form.Label>
          <Form.Control
            type="text"
            value={this.state.areasOfInterest}
            onChange={this.handleFormChange}
          />
        </Form.Group>
        <Form.Group controlId="intendedUse" size="lg">
          <Form.Label>Brief description of the intended use of images (no more than 500 words)</Form.Label>
          <Form.Control as="textarea"
                        rows={5}
                        value={this.state.intendedUse}
                        onChange={this.handleFormChange}/>
        </Form.Group>
        {this.renderProfileChangeSubmitButton()}
      </Form>
    )
  }

  renderProfileChangeSubmitButton() {
    throw new Error('Must implement renderProfileChangeSubmitButton()!')
  }

  render() {
    throw new Error('Must implement render()!')
  }
}

ProfileManagement.contextType = AppContext

export default ProfileManagement
