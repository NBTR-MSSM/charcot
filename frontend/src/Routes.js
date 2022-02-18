import React, { Component } from 'react'
import { Route, Switch } from 'react-router-dom'
import Home from './containers/Home'
import Cart from './containers/Cart'
import Checkout from './containers/Checkout'
import Search from './containers/Search'
import NotFound from './containers/NotFound'

export default class Routes extends Component {
  render () {
    return (
      <Switch>
        <Route exact path="/">
          <Home/>
        </Route>
        <Route exact path="/search">
          <Search images={this.props.images} onImageClick={this.props.onImageClick}
                  onImageSearch={this.props.onImageSearch}/>
        </Route>
        <Route exact path="/cart">
          <Cart selectedImages={this.props.images.filter(image => image.isSelected)}
                onImageClick={this.props.onImageClick}/>
        </Route>
        <Route exact path="/checkout">
          <Checkout/>
        </Route>
        {/* Finally, catch all unmatched routes */}
        <Route>
          <NotFound/>
        </Route>
      </Switch>)
  }
}
