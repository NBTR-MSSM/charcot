import React, { Component } from 'react'
import Container from 'react-bootstrap/Container'
import Row from 'react-bootstrap/Row'
import Col from 'react-bootstrap/Col'
import './Checkout.css'

const dimensions = ['age', 'sex', 'region', 'stain', 'race']

export default class Checkout extends Component {
  componentDidMount () {
    this.props.onRouteLoad({
      active: 'checkout'
    })
  }

  render = () => {
    return (
      <Container className='Checkout' bsPrefix={'charcot-checkout-container'}>
        <h3>The Data</h3>
        {dimensions.map((e, index) => {
          return <Row key={index} className='row'
                      bsPrefix={'charcot-checkout-container-row'}><Col>{e.substring(0, 1).toUpperCase() + e.substring(1)}</Col><Col>X
            selections</Col></Row>
        })}
      </Container>)
  }
}
