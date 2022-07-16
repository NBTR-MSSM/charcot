import React, { Component } from 'react'
import Form from 'react-bootstrap/Form'
import './Login.css'
import { Auth } from 'aws-amplify'
import { AppContext } from '../lib/context'
import LoaderButton from '../components/LoaderButton'

class Login extends Component {
  constructor (props) {
    super(props)
    this.state = {
      email: '',
      password: '',
      isLoading: false
    }
  }

  validateForm = () => this.state.email.length > 0 && this.state.password.length > 0

  handleSubmit = async (event) => {
    event.preventDefault()
    this.setState(
      { isLoading: true }
    )
    const { email, password } = this.state
    try {
      await Auth.signIn(email, password)
      // eslint-disable-next-line no-undef
      this.context.handleLogin()
      this.context.redirect({ to: this.context.routeState.active })
    } catch (e) {
      // eslint-disable-next-line no-undef
      alert(e.message)
    }
    this.setState(
      { isLoading: false }
    )
  }

  handleFormChange = (event) => {
    const newState = {}
    const { name, value } = event.target
    newState[name] = value
    this.setState(newState)
  }

  render () {
    // console.log(`JMQ: this.context.isAuthenticated is ${this.context.isAuthenticated}`)
    return (
      <div className='Login'>
        <Form onSubmit={this.handleSubmit}>
          <Form.Group size="lg" controlId="email">
            <Form.Label>Email</Form.Label>
            <Form.Control
              autoFocus
              type='email'
              name='email'
              value={this.state.email}
              onChange={this.handleFormChange}
            />
          </Form.Group>
          <Form.Group size="lg" controlId="password">
            <Form.Label>Password</Form.Label>
            <Form.Control
              type='password'
              name='password'
              value={this.state.password}
              onChange={this.handleFormChange}
            />
          </Form.Group>
          <LoaderButton id='submit-btn' block='false' size='lg' type="submit" isLoading={this.state.isLoading}
                        disabled={!this.validateForm()}>
            Login
          </LoaderButton>
        </Form>
      </div>
    )
  }
}

Login.contextType = AppContext

export default Login
