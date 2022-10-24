import ProfileManagement from './ProfileManagement'
import { API } from 'aws-amplify'
import { onError } from '../lib/error'
import LoaderButton from '../components/LoaderButton'
import React from 'react'

class EditUser extends ProfileManagement {
  componentDidUpdate() {
    console.log('JMQ: other user is EditUser componentDidUpdate()')
  }

  async componentDidMount() {
    const user = await this.retrieveUserDetails()
    console.log(`JMQ: other user is ${JSON.stringify(user)}`)
    this.setState({
      ...user
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
          ...this.userAttributes()
        }
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
        block="true"
        size="lg"
        type="submit"
        variant="success"
        isLoading={this.state.isLoading}
        disabled={!this.validateForm()}>
        Update
      </LoaderButton>
    )
  }

  async retrieveUserDetails() {
    return await API.get('charcot', `/cerebrum-image-users/${this.context.otherUserEmail}`, undefined)
  }
}

export default EditUser
