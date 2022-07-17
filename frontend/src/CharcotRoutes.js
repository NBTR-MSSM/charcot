import React, { Component } from 'react'
import { Route, Switch } from 'react-router-dom'
import Home from './containers/Home'
import Review from './containers/Review'
import Search from './containers/Search'
import NotFound from './containers/NotFound'
import Login from './containers/Login'
import Signup from './containers/Signup'

export default class CharcotRoutes extends Component {
  render () {
    return (
      <Switch>
        <Route exact path='/'>
          <Home/>
        </Route>
        <Route exact path='/search'>
          <Search onRouteLoad={this.props.onRouteLoad}
                  onCategorySelect={this.props.onCategorySelect}
                  onCategoryUnselect={this.props.onCategoryUnselect}
                  onClearFilter={this.props.onClearFilter}
                  filter={this.props.filter}
                  dimensionData={this.props.dimensionData}/>
        </Route>
        <Route exact path='/review'>
          <Review onRouteLoad={this.props.onRouteLoad}
                  onCategorySelect={this.props.onCategorySelect}
                  onCategoryUnselect={this.props.onCategoryUnselect}
                  filter={this.props.filter}
                  dimensionData={this.props.dimensionData}/>
        </Route>
        <Route exact path='/signup'>
          <Signup />
        </Route>
        <Route exact path='/login'>
          <Login />
        </Route>
        {/* Finally, catch all unmatched routes */}
        <Route>
          <NotFound/>
        </Route>
      </Switch>)
  }
}
