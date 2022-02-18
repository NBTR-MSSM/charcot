import React, { Component } from 'react'
import Nav from 'react-bootstrap/Nav'
import Navbar from 'react-bootstrap/Navbar'
import './App.css'
import Routes from './Routes'
import { LinkContainer } from 'react-router-bootstrap'
import { API } from 'aws-amplify'

const savedState = {
  images: []
}

export default class App extends Component {
  constructor (props) {
    super(props)
    this.state = {
      images: []
    }
  }

  async componentDidMount () {
    this.updateState({
      images: await API.get('charcot', '/cerebrum-images?region=Orbital Frontal Cortex', {})
    })
  }

  updateState ({ images }) {
    // We use a Map to de-dup images
    savedState.images = Array.from(new Map(images.map(e => [e.fileName, e])).values())
    this.setState({
      images: savedState.images
    })
  }

  /**
   * If image is clicked and it had already been selected, remove
   * from "selected" array, else add it.
   */
  handleImageClick = (fileName) => {
    const found = savedState.images.find(e => e.fileName === fileName)
    // If item had previously been selected, toggle it unselected, and vice-versa
    found.isSelected = !found.isSelected
    this.updateState(savedState)
  }

  /**
   * When downstream image selection changes based on search,
   * update our UI, retaining those that had been previously selected.
   */
  handleImageSearch = (images) => {
    this.updateState({ images: images.concat(savedState.images.filter(e => e.isSelected)) })
  }

  render () {
    return (
      <div className="App container py-3">
        <Navbar collapseOnSelect bg="light" expand="md" className="mb-3">
          <LinkContainer to="/">
            <Navbar.Brand className="font-weight-bold text-muted">
              Charcot
            </Navbar.Brand>
          </LinkContainer>
          <Navbar.Toggle/>
          <Navbar.Collapse className="justify-content-end">
            <Nav activeKey={window.location.pathname}>
              <LinkContainer to="/search">
                <Nav.Link>Search</Nav.Link>
              </LinkContainer>
              <LinkContainer to="/cart">
                <Nav.Link>Cart ({savedState.images.filter(e => e.isSelected).length})</Nav.Link>
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
        <Routes images={savedState.images} onImageClick={this.handleImageClick} onImageSearch={this.handleImageSearch}/>
      </div>)
  }
}
