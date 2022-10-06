import { Component } from 'react'
import { AppContext } from '../lib/context'

const attributeOrder = [
  'Degree',
  'Institution Address',
  'Institution Name',
  'Areas Of Interest',
  'Intended Use']

class TransactionItem extends Component {
  render() {
    const item = this.props.item
    return (
      <tr>
        <td>{item.orderId}</td>
        <td>{new Date(item.created).toUTCString()}</td>
        <td>{item.email}</td>
        <td>{item.filter}</td>
        <td>{attributeOrder.map(attrName => <span className="userAttribute"><span className="userAttributeName">{attrName}</span>: {item.userAttributes[attrName]}</span>)}</td>
      </tr>
    )
  }
}

TransactionItem.contextType = AppContext

export default TransactionItem
