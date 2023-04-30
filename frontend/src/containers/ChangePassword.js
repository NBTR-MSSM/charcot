import ProfileManagement from './ProfileManagement'
import LoaderButton from '../components/LoaderButton'
import React from 'react'
import './ForgotPassword.css'
import { FormControl, FormGroup, FormLabel } from 'react-bootstrap'
import { Auth } from 'aws-amplify'
import { onError } from '../lib/error'
import Button from 'react-bootstrap/Button'
import './ChangePassword.css'

export default class ForgotPassword extends ProfileManagement {
  constructor(props) {
    super(props)
    this.state = {
      ...this.state,
      oldPassword: ''
    }
  }

  renderProfileChangeSubmitButton() {
    return (
      <LoaderButton
        id="change-password-btn"
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
      this.state.oldPassword.length > 0 &&
      this.state.password.length > 0 &&
      this.state.password === this.state.confirmPassword
    )
  }

  handleChangePasswordSubmit = async (event) => {
    event.preventDefault()
    this.setState({
      isLoading: true
    })
    const {
      password,
      oldPassword
    } = this.state
    try {
      const currentUser = await Auth.currentAuthenticatedUser()
      await Auth.changePassword(
        currentUser,
        oldPassword,
        password
      )
      this.context.redirect({ to: '/search' })
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

  render() {
    return <div className="ChangePassword">
      <Button id="back-btn" size="sm" onClick={() => this.context.redirectToPrevious()}>{'< Back'}</Button>
      <form onSubmit={this.handleChangePasswordSubmit}>
        <FormGroup bsSize="large" controlId="oldPassword">
          <FormLabel>Old Password</FormLabel>
          <FormControl
            type="password"
            onChange={this.handleFormChange}
            value={this.state.oldPassword}
          />
        </FormGroup>
        <hr/>
        <FormGroup bsSize="large" controlId="password">
          <FormLabel>New Password</FormLabel>
          <FormControl
            type="password"
            onChange={this.handleFormChange}
            value={this.state.password}
          />
        </FormGroup>
        <FormGroup bsSize="large" controlId="confirmPassword">
          <FormLabel>Confirm Password</FormLabel>
          <FormControl
            type="password"
            onChange={this.handleFormChange}
            value={this.state.confirmPassword}
          />
        </FormGroup>
        {this.renderProfileChangeSubmitButton()}
        <Button className="cancel" variant="secondary" size="sm"
                onClick={() => this.context.redirectToPrevious()}>
          Cancel
        </Button>
      </form>
    </div>
  }
}
