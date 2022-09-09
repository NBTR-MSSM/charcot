import React, { Component } from 'react'
import { Route, Switch } from 'react-router-dom'
import Home from './containers/Home'
import Review from './containers/Review'
import Search from './containers/Search'
import NotFound from './containers/NotFound'
import Login from './containers/Login'
import Signup from './containers/Signup'
import Confirmation from './containers/Confirmation'
import Splash from './containers/Splash'

export default class CharcotRoutes extends Component {
  render () {
    return (
      <Switch>
        <Route exact path='/'>
          <Splash/>
        </Route>
        <Route exact path='/home'>
          <Home/>
        </Route>
        <Route exact path='/search'>
          <Search filter={this.props.filter}/>
        </Route>
        <Route exact path='/review'>
          <Review filter={this.props.filter}/>
        </Route>
        <Route exact path='/signup'>
          <Signup />
        </Route>
        <Route exact path='/login'>
          <Login />
        </Route>
        <Route exact path='/confirmation'>
          <Confirmation />
        </Route>
        {/* Finally, catch all unmatched routes */}
        <Route>
          <NotFound/>
        </Route>
      </Switch>)
  }
}
