import React, { Component } from 'react'
import Form from 'react-bootstrap/Form'
import LoaderButton from '../components/LoaderButton'
import { AppContext } from '../lib/context'
import { onError } from '../lib/error'
import './Signup.css'
import { Auth } from 'aws-amplify'

class Signup extends Component {
  constructor (props) {
    super(props)
    this.state = {
      email: '',
      password: '',
      confirmPassword: '',
      confirmationCode: '',
      newUser: '',
      isLoading: false
    }
  }

  validateForm = () => {
    return (
      this.state.email.length > 0 &&
      this.state.password.length > 0 &&
      this.state.password === this.state.confirmPassword
    )
  }

  validateConfirmationForm = () => {
    return this.state.confirmationCode.length > 0
  }

  handleSubmit = async (event) => {
    event.preventDefault()
    this.setState({
      isLoading: true
    })
    const { email, password } = this.state
    try {
      const newUser = await Auth.signUp({
        username: email,
        password
      })
      this.setState({
        isLoading: true,
        newUser
      })
    } catch (e) {
      onError(e)
    }
    this.setState({
      isLoading: false
    })
  }

  handleConfirmationSubmit = async (event) => {
    event.preventDefault()
    this.setState({
      isLoading: true
    })
    const { confirmationCode, email, password } = this.state

    try {
      await Auth.confirmSignUp(email, confirmationCode)
      await Auth.signIn(email, password)
      this.context.handleLogin()
      // send them back to whatever page they were one when they chose to sign up
      this.context.redirect({ to: this.context.routeState.active })
    } catch (e) {
      onError(e)
    }

    this.setState({
      isLoading: false
    })
  }

  handleFormChange = (event) => {
    const newState = {}
    const { id, value } = event.target
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

  renderForm = () => {
    return (
      <Form onSubmit={this.handleSubmit}>
        <Form.Group controlId="email" size="lg">
          <Form.Label>Email</Form.Label>
          <Form.Control
            autoFocus
            type="email"
            value={this.state.email}
            onChange={this.handleFormChange}
          />
        </Form.Group>
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
        <LoaderButton
          block="true"
          size="lg"
          type="submit"
          variant="success"
          isLoading={this.state.isLoading}
          disabled={!this.validateForm()}>
          Signup
        </LoaderButton>
      </Form>
    )
  }

  render () {
    return (
      <div className="Signup">
        {this.state.newUser ? this.renderConfirmationForm() : this.renderForm()}
      </div>
    )
  }
}

Signup.contextType = AppContext

export default Signup
