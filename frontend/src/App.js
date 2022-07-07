import React, { Component } from 'react'
import Nav from 'react-bootstrap/Nav'
import Navbar from 'react-bootstrap/Navbar'
import './App.css'
import Routes from './Routes'
import { LinkContainer } from 'react-router-bootstrap'
import Footer from './containers/Footer'
import LeftNav from './containers/LeftNav'
import 'bootstrap/dist/css/bootstrap.min.css'
import Stack from 'react-bootstrap/Stack'
import { dataService } from './lib/DataService'
import Filter from './lib/Filter'

const savedState = {
  filter: new Filter()
}

export default class App extends Component {
  constructor (props) {
    super(props)
    this.state = {
      routeState: {},
      filter: new Filter(),
      dimensionData: []
    }
  }

  async componentDidMount () {
    console.log('App mounted')
    await this.updateState({ filter: this.state.filter })
  }

  /**
   * Updates the filter with the selected dimension/category and
   * refreshes the state
   */
  handleCategorySelect = async ({ dimension, category }) => {
    const filter = this.state.filter
    console.log(`JMQ: pre handleCategorySelect filter is ${filter.serialize()}`)
    filter.add({ dimension, category })
    console.log(`JMQ: post handleCategorySelect filter is ${filter.serialize()}`)
    await this.updateState({ filter })
  }

  /**
   * Does the opposite of 'Search.handleSelect'
   * and refreshes the state.
   */
  handleCategoryUnselect = async ({ dimension, category }) => {
    const filter = this.state.filter
    console.log(`JMQ: pre handleCategoryUnselect filter is ${filter.serialize()}`)
    filter.remove({ dimension, category })

    console.log(`JMQ: post handleCategoryUnselect filter is ${filter.serialize()}`)
    await this.updateState({ filter })
  }

  handleClearFilter = async () => {
    await this.updateState({ filter: this.state.filter.clear() })
  }

  handleRouteLoad = (routeState) => {
    this.setState({
      routeState: { ...routeState }
    })
  }

  async updateState ({ filter }) {
    const dimensionData = await dataService.fetchAll({
      filter
    })

    // console.log(`JMQ: dimensionData is ${JSON.stringify(dimensionData)}`)
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
    console.log('JMQ: rendering App')
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
    return (
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
                  <LinkContainer to="/checkout">
                    <Nav.Link>Checkout</Nav.Link>
                  </LinkContainer>
                  <LinkContainer to="/signup">
                    <Nav.Link>Signup</Nav.Link>
                  </LinkContainer>
                  <LinkContainer to="/login">
                    <Nav.Link>Login</Nav.Link>
                  </LinkContainer>
                </Nav>
              </Navbar.Collapse>
            </Navbar>
          </div>
        </Stack>
        <Routes onCategorySelect={this.handleCategorySelect}
                onCategoryUnselect={this.handleCategoryUnselect}
                onClearFilter={this.handleClearFilter}
                onRouteLoad={this.handleRouteLoad} filter={savedState.filter.clone()}
                dimensionData={this.state.dimensionData}/>
        {footer}
      </div>)
  }
}
