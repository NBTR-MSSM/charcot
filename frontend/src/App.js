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
 * TODO: Methodology for redirect upon login/logout a bit convoluted. Each route sets the routeState, then upon
 *       login/logout we redirect to the last known route. Is there a cleaner way of doing it?
 */
export default class App extends Component {
  constructor (props) {
    super(props)
    this.state = {
      isAuthenticated: false,
      isAuthenticating: true,
      routeState: {},
      filter: new Filter(),
      dimensionData: [],
      handleLogin: this.handleLogin,
      handleLogout: this.handleLogout,
      redirect: this.redirect,
      handleRouteLoad: this.handleRouteLoad,
      redirectTo: ''
    }
  }

  async componentDidMount () {
    console.log('App mounted')
    await this.onLoad()
  }

  componentDidUpdate () {
    if (this.state.redirectTo) {
      this.setState({
        redirectTo: ''
      })
    }
  }

  onLoad = async () => {
    // Load user session if any (I.e. if user is already logged in)
    try {
      await Auth.currentSession()
      this.handleLogin()
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

  handleLogin = () => {
    savedState.isAuthenticated = true
    this.setState(
      {
        isAuthenticated: savedState.isAuthenticated
      }
    )
  }

  redirect = ({ to }) => {
    this.setState(
      {
        redirectTo: to
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

  handleRouteLoad = (routeState) => {
    this.setState({
      routeState: { ...routeState }
    })
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
    if (this.state.redirectTo) {
      return <Redirect to={`/${this.state.redirectTo === 'home' ? '' : this.state.redirectTo}`}/>
    }
    let leftNav
    if (this.state.routeState.active === 'search') {
      leftNav = <div><LeftNav dimensionData={this.state.dimensionData}
                              onCategorySelect={this.handleCategorySelect}
                              onCategoryUnselect={this.handleCategoryUnselect}/></div>
    }

    let footer
    if (this.state.routeState.active === 'search' || this.state.routeState.active === 'checkout') {
      footer =
        <Footer isCheckout={this.state.routeState.active === 'checkout'} filter={savedState.filter.clone()}
                dimensionData={this.state.dimensionData}/>
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

    return !this.state.isAuthenticating && (
      <div className='App container py-3'>
        <Stack direction="horizontal" gap={3}>
          {leftNav}
          <div>
            <Navbar collapseOnSelect bg="light" expand="md" className="mb-3 fixed-top charcot-top-nav">
              <LinkContainer to="/">
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
        <AppContext.Provider value={this.state}>
          <CharcotRoutes onCategorySelect={this.handleCategorySelect}
                         onCategoryUnselect={this.handleCategoryUnselect}
                         onClearFilter={this.handleClearFilter}
                         onRouteLoad={this.handleRouteLoad} filter={savedState.filter.clone()}
                         dimensionData={this.state.dimensionData}/>
          {footer}
        </AppContext.Provider>
      </div>)
  }
}
