import ProfileManagement from './ProfileManagement'
import { Auth } from 'aws-amplify'
import { onError } from '../lib/error'
import LoaderButton from '../components/LoaderButton'
import React from 'react'

export default class Signup extends ProfileManagement {
  handleProfileChangeSubmit = async (event) => {
    event.preventDefault()
    this.setState({
      isLoading: true
    })
    const {
      email,
      password
    } = this.state
    try {
      const newUser = await Auth.signUp({
        username: email,
        password,
        attributes: {
          ...this.userAttributes()
        }
      })
      this.setState({
        newUser
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
      <LoaderButton
        id="signup-submit-btn"
        block="true"
        size="md"
        type="submit"
        isLoading={this.state.isLoading}
        disabled={!this.validateForm()}>
        Signup
      </LoaderButton>
    )
  }

  render() {
    return (
      <div className="ProfileManagement">
        {this.state.newUser ? this.renderConfirmationForm() : this.renderProfileChangeForm()}
      </div>
    )
  }
}
