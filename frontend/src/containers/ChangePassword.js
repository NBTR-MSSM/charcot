import ProfileManagement from './ProfileManagement'
import LoaderButton from '../components/LoaderButton'
import React from 'react'
import './ForgotPassword.css'
import { FormControl, FormGroup, FormLabel } from 'react-bootstrap'
import { Auth } from 'aws-amplify'
import { onError } from '../lib/error'
import Button from 'react-bootstrap/Button'
import './ChangePassword.css'

export default class ChangePassword extends ProfileManagement {
  constructor(props) {
    super(props)
    this.state = {
      ...this.state,
      oldPassword: ''
    }
  }

  isNewPasswordRequiredMode = () => new URLSearchParams(window.location.search).get('newPasswordRequired')

  renderProfileChangeSubmitButton() {
    return (
      <LoaderButton
        id="change-password-btn"
        block="true"
        size="sm"
        type="submit"
        isLoading={this.state.isLoading}
        disabled={!this.validateForm()}>
        Change It
      </LoaderButton>
    )
  }

  validateForm() {
    return (
      (this.state.oldPassword.length || this.isNewPasswordRequiredMode()) > 0 &&
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
      if (this.isNewPasswordRequiredMode()) {
        await Auth.completeNewPassword(
          this.context.sessionInfo,
          password
        )
        this.context.redirect({ to: '/login' })
      } else {
        await Auth.changePassword(
          await Auth.currentAuthenticatedUser(),
          oldPassword,
          password
        )
        this.context.redirect({ to: '/search' })
      }
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
    let oldPasswordFragment = <>
      <FormGroup bsSize="large" controlId="oldPassword">
        <FormLabel>Old Password</FormLabel>
        <FormControl
          type="password"
          onChange={this.handleFormChange}
          value={this.state.oldPassword}
        />
      </FormGroup>
      <hr/>
    </>

    if (this.isNewPasswordRequiredMode()) {
      oldPasswordFragment = <h2>You need to update your password</h2>
    }
    return <div className="ChangePassword">
      <Button id="back-btn" size="sm" onClick={() => this.context.redirectToPrevious()}>{'< Back'}</Button>
      <form onSubmit={this.handleChangePasswordSubmit}>
        {oldPasswordFragment}
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
