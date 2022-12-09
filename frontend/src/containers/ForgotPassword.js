import ProfileManagement from './ProfileManagement'
import LoaderButton from '../components/LoaderButton'
import React from 'react'
import './ForgotPassword.css'
import { Form } from 'react-bootstrap'
import { Auth } from 'aws-amplify'
import { onError } from '../lib/error'

export default class ForgotPassword extends ProfileManagement {
  constructor(props) {
    super(props)
    this.state = {
      ...this.state,
      isForgotPasswordRequestSent: false
    }
  }

  renderProfileChangeSubmitButton() {
    return (
      <LoaderButton
        id="forgot-password-btn"
        block="true"
        size="sm"
        type="submit"
        isLoading={this.state.isLoading}
        disabled={!this.validateForm()}>
        Send request
      </LoaderButton>
    )
  }

  validateForm() {
    return (
      this.state.email.length > 0
    )
  }

  validateResetPasswordForm() {
    return (
      this.state.password.length > 0 &&
      this.state.password === this.state.confirmPassword
    )
  }

  handleForgotPasswordRequest = async (event) => {
    event.preventDefault()
    this.setState({
      isLoading: true
    })
    try {
      await Auth.forgotPassword(this.state.email)
      this.setState({
        isForgotPasswordRequestSent: true
      })
    } catch (e) {
      onError(e)
    }
    this.setState({
      isLoading: false
    })
  }

  handleResetPasswordSubmit = async (event) => {
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
      await Auth.forgotPasswordSubmit(email, confirmationCode, password)
      this.context.redirect({ to: '/login' })
    } catch (e) {
      onError(e)
    }
    this.setState({
      isLoading: false
    })
  }

  renderProfileChangeForm() {
    return <></>
  }

  renderForgotPasswordForm() {
    return <Form onSubmit={this.handleForgotPasswordRequest}>
      {this.renderEmailField()}
      {this.renderProfileChangeSubmitButton()}
    </Form>
  }

  renderResetPasswordForm() {
    return (
      <Form onSubmit={this.handleResetPasswordSubmit}>
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
        {this.renderPasswordFields()}
        <LoaderButton
          id="reset-password-btn"
          size="md"
          type="submit"
          isLoading={this.state.isLoading}
          disabled={!this.validateResetPasswordForm()}>
          Reset
        </LoaderButton>
      </Form>
    )
  }

  render() {
    return <div className="ForgotPassword">
      {this.state.isForgotPasswordRequestSent ? this.renderResetPasswordForm() : this.renderForgotPasswordForm()}
    </div>
  }
}
