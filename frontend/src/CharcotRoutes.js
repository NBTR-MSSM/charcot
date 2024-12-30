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
import Transaction from './containers/Transaction'
import { AppContext } from './lib/context'
import EditUser from './containers/EditUser'
import ForgotPassword from './containers/ForgotPassword'
import ChangePassword from './containers/ChangePassword'
import TransactionDetail from './containers/TransactionDetail'

class CharcotRoutes extends Component {
  render() {
    const isOrderApproval = new URLSearchParams(window.location.search).get('orderApproval')

    return (
      <Switch>
        <Route exact path="/">
          <Splash/>
        </Route>
        <Route exact path="/home">
          <Home/>
        </Route>
        <Route exact path="/search">
          <Search filter={this.props.filter}/>
        </Route>
        <Route exact path="/review">
          <Review filter={this.props.filter}/>
        </Route>
        <Route exact path="/signup">
          <Signup/>
        </Route>
        <Route exact path="/login">
          <Login/>
        </Route>
        <Route exact path="/forgot-password">
          <ForgotPassword/>
        </Route>
        <Route exact path="/change-password">
          <ChangePassword/>
        </Route>
        <Route exact path="/confirmation">
          <Confirmation/>
        </Route>
        {/* Leave access control up to Transaction screen: Relax access to '/transaction' screen to support arrival from request
        approval email because the approver might not be logged in,
        BUT that component restricts access to logged in admins, see 'componentDidMount()' of 'Transaction.js' for details */
          (this.context.isAdmin || isOrderApproval) && <Route exact path="/transaction"><Transaction/></Route>
        }
        {
          (this.context.isAdmin) && <>
            <Route exact path="/transaction-detail"><TransactionDetail/></Route>
            <Route exact path="/edit-user"><EditUser/></Route>
          </>
        }
        {/* Finally, catch all unmatched routes */}
        <Route>
          <NotFound/>
        </Route>
      </Switch>)
  }
}

CharcotRoutes.contextType = AppContext

export default CharcotRoutes
