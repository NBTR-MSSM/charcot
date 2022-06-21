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

const savedState = {
  filter: {}
}

export default class App extends Component {
  constructor (props) {
    super(props)
    this.state = {
      routeState: {},
      filter: {},
      dimensionData: [],
      updatedDimension: undefined
    }
  }

  async componentDidMount () {
    console.log('App mounted')
    await this.updateState({ filter: this.state.filter, dimension: this.state.updatedDimension })
  }

  cloneFilter = () => {
    const clone = {}
    for (const tup of Object.entries(this.state.filter)) {
      const newSet = new Set()
      tup[1].forEach((val) => newSet.add(val))
      clone[tup[0]] = newSet
    }
    return clone
  }

  /**
   * Updates the filter with the selected dimension/category and
   * refreshes the state
   */
  handleCategorySelect = async ({ dimension, category }) => {
    const filter = this.state.filter
    console.log(`JMQ: pre handleCategorySelect filter is ${Object.entries(filter).map(tup => [tup[0], ...tup[1]])}`)
    let categories = filter[dimension]
    if (!categories) {
      categories = new Set()
      filter[dimension] = categories
    }
    categories.add(category)
    console.log(`JMQ: post handleCategorySelect filter is ${Object.entries(filter).map(tup => [tup[0], ...tup[1]])}`)
    await this.updateState({ filter, dimension })
  }

  /**
   * Does the opposite of 'Search.handleSelect'
   * and refreshes the state.
   */
  handleCategoryUnselect = async ({ dimension, category }) => {
    const filter = this.state.filter
    console.log(`JMQ: pre handleCategoryUnselect filter is ${Object.entries(filter).map(tup => [tup[0], ...tup[1]])}`)
    filter[dimension].delete(category)

    // Delete this dimension from the object if
    // this was the only selected category
    if (!filter[dimension].size) {
      delete filter[dimension]
    }

    console.log(`JMQ: post handleCategoryUnselect filter is ${Object.entries(filter).map(tup => [tup[0], ...tup[1]])}`)
    await this.updateState({ filter, dimension })
  }

  handleRouteLoad = (routeState) => {
    this.setState({
      routeState: { ...routeState }
    })
  }

  async updateState ({ filter, dimension }) {
    const dimensionData = await dataService.fetchAll({
      filter
    })

    // console.log(`JMQ: dimensionData is ${JSON.stringify(dimensionData)}`)
    savedState.filter = filter
    this.setState({
      filter: savedState.filter,
      updatedDimension: dimension,
      dimensionData
    })
  }

  /**
   * We take care to send downstream a clone of the filter to avoid the pitfall
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
        <Footer isCheckout={this.state.routeState.active === 'checkout'} filter={this.cloneFilter(savedState.filter)}
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
                onRouteLoad={this.handleRouteLoad} filter={this.cloneFilter(savedState.filter)}
                updatedDimension={this.state.updatedDimension} dimensionData={this.state.dimensionData}/>
        {footer}
      </div>)
  }
}
