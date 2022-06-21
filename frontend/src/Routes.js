import React, { Component } from 'react'
import { Route, Switch } from 'react-router-dom'
import Home from './containers/Home'
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
          <Search onRouteLoad={this.props.onRouteLoad} onCategorySelect={this.props.onCategorySelect}
                  onCategoryUnselect={this.props.onCategoryUnselect} filter={this.props.filter}
                  updatedDimension={this.props.updatedDimension}
                  dimensionData={this.props.dimensionData}/>
        </Route>
        <Route exact path="/checkout">
          <Checkout onRouteLoad={this.props.onRouteLoad} filter={this.props.filter} dimensionData={this.props.dimensionData}/>
        </Route>
        {/* Finally, catch all unmatched routes */}
        <Route>
          <NotFound/>
        </Route>
      </Switch>)
  }
}
