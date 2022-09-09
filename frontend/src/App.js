import React, { Component } from 'react'
import Nav from 'react-bootstrap/Nav'
import Navbar from 'react-bootstrap/Navbar'
import './App.css'
import CharcotRoutes from './CharcotRoutes'
import { LinkContainer } from 'react-router-bootstrap'
import Footer from './containers/Footer'
import LeftNav from './containers/LeftNav'
import 'bootstrap/dist/css/bootstrap.min.css'
import Stack from 'react-bootstrap/Stack'
import { dataService } from './lib/DataService'
import Filter from './lib/Filter'
import { AppContext } from './lib/context'
import { Auth } from 'aws-amplify'
import { Redirect } from 'react-router-dom'
import { onError } from './lib/error'

const savedState = {
  filter: new Filter()
}

/**
 * TODO: Move all the various handlers that are passed down the component hierarchy via props to AppContext,
 *       and clean up all those unnecessary props.
 */
export default class App extends Component {
  constructor (props) {
    super(props)
    this.state = {
      isAuthenticated: false,
      isAuthenticating: true,
      email: '',
      filter: new Filter(),
      dimensionData: {
        dimensions: []
      },
      handleCategorySelect: this.handleCategorySelect,
      handleCategoryUnselect: this.handleCategoryUnselect,
      handleClearFilter: this.handleClearFilter,
      handleLogin: this.handleLogin,
      handleLogout: this.handleLogout,
      redirect: this.redirect,
      currentPage: this.currentPage,
      redirectTo: '',
      redirectToPrevious: this.redirectToPrevious,
      navHistory: [],
      pushToHistory: this.pushToHistory
    }
  }

  async componentDidMount () {
    await this.onLoad()
  }

  pushToHistory = () => {
    const history = this.state.navHistory
    history.push(window.location.pathname)
    this.setState({
      navHistory: history
    })
  }

  componentDidUpdate () {
    console.log('JMQ: App did update ')
    if (this.state.redirectTo) {
      this.setState({
        redirectTo: ''
      })
    }
  }

  redirectToPrevious = () => {
    const history = this.state.navHistory
    this.redirect({ to: history[history.length - 2] })
  }

  onLoad = async () => {
    // Load user session if any (I.e. if user is already logged in)
    try {
      await Auth.currentSession()
      const user = await Auth.currentUserInfo()
      this.handleLogin({ email: user.attributes.email })
    } catch (e) {
      if (e !== 'No current user') {
        onError(e)
      }
    }

    this.setState({
      isAuthenticating: false
    })
    await this.updateChartDataState({ filter: this.state.filter })
  }

  handleLogin = ({ email }) => {
    this.setState(
      {
        email,
        isAuthenticated: true
      }
    )
  }

  redirect = ({ to }) => {
    this.setState(
      {
        redirectTo: to || 'home'
      }
    )
  }

  handleLogout = async () => {
    await Auth.signOut()
    this.setState(
      {
        isAuthenticated: false
      }
    )
    this.redirect({ to: '/login' })
  }

  /**
   * Updates the filter with the selected dimension/category and
   * refreshes the state
   */
  handleCategorySelect = async ({ dimension, category }) => {
    const filter = this.state.filter
    filter.add({ dimension, category })
    await this.updateChartDataState({ filter })
  }

  /**
   * Does the opposite of 'Search.handleSelect'
   * and refreshes the state.
   */
  handleCategoryUnselect = async ({ dimension, category }) => {
    const filter = this.state.filter
    filter.remove({ dimension, category })
    await this.updateChartDataState({ filter })
  }

  handleClearFilter = async () => {
    await this.updateChartDataState({ filter: this.state.filter.clear() })
  }

  currentPage = () => {
    const history = this.state.navHistory
    return history.length > 0 && history[history.length - 1]
  }

  previousPage = () => {
    const history = this.state.navHistory
    return history.length > 1 && history[history.length - 2]
  }

  /**
   * Triggers state changes that effect the charts (I.e. cause chart components
   * to re-render themselves)
   */
  async updateChartDataState ({ filter }) {
    const dimensionData = await dataService.fetchAll({
      filter
    })

    savedState.filter = filter
    this.setState({
      filter: savedState.filter,
      dimensionData
    })
  }

  /**
   * We take care to send downstream a clone (I.e. a copy) of the filter to avoid the pitfall
   * that ensues in scenarios where child components compare previous and current
   * props to decide if the should update themselves, see
   * https://stackoverflow.com/questions/52393172/comparing-prevprops-in-componentdidupdate,
   * search for "when you go to do a comparison you are comparing the two exact same arrays ALWAYS"
   */
  render () {
    /*
     * This is a (somewhat convoluted) way to handle redirect back to page of origin
     * during signup/login and any other scenario where applicable. The componentDidUpdate()
     * lifecycle method clears this.state.redirectTo to avoid infinite redirect!
     */
    if (this.state.redirectTo) {
      return <Redirect to={this.state.redirectTo}/>
    }
    let leftNav
    if (this.currentPage() === '/search') {
      leftNav = <div><LeftNav/></div>
    }

    let footer
    if (this.currentPage() === '/search' || this.currentPage() === '/review') {
      footer =
        <Footer filter={savedState.filter.clone()}/>
    }

    let authFragment = <><LinkContainer to="/signup">
      <Nav.Link>Signup</Nav.Link>
    </LinkContainer>
      <LinkContainer to="/login">
        <Nav.Link>Login</Nav.Link>
      </LinkContainer>
    </>

    if (this.state.isAuthenticated) {
      authFragment = <Nav.Link onClick={this.handleLogout}>Logout</Nav.Link>
    }

    console.log(`JMQ: currentPage is ${this.currentPage()}`)

    return !this.state.isAuthenticating && (
      <div className='App container py-3'>
        <AppContext.Provider value={this.state}>
          <Stack hidden={this.currentPage() === '/'} direction="horizontal" gap={3}>
            {leftNav}
            <div>
              <Navbar collapseOnSelect bg="light" expand="md" className="mb-3 fixed-top charcot-top-nav">
                <LinkContainer to="/home">
                  <Navbar.Brand className="font-weight-bold text-muted">
                    Mount Sinai Brain Slide
                  </Navbar.Brand>
                </LinkContainer>
                <Navbar.Toggle/>
                <Navbar.Collapse className="justify-content-end">
                  <Nav activeKey={window.location.pathname}>
                    <LinkContainer to="/search">
                      <Nav.Link>Search</Nav.Link>
                    </LinkContainer>
                    {authFragment}
                  </Nav>
                </Navbar.Collapse>
              </Navbar>
            </div>
          </Stack>
          <CharcotRoutes filter={savedState.filter.clone()}/>
          {footer}
        </AppContext.Provider>
      </div>)
  }
}
