import { Component } from 'react'
import { AppContext } from '../lib/context'
import { Card, OverlayTrigger } from 'react-bootstrap'
import { BsInfoCircleFill } from 'react-icons/bs'

const attributeOrder = ['degree', 'institutionName', 'institutionAddress', 'areasOfInterest', 'intendedUse']

class TransactionItem extends Component {
  // <td>{attributeOrder.map(attrName => <span className="userAttribute"><span className="userAttributeName">{attrName}</span>: {item.userAttributes[attrName]}</span>)}</td>
  render() {
    const item = this.props.item
    const userAttributesPopover = (
      <Card body style={{ width: '425px' }}>
      <span className="userAttribute"><span className="userAttributeName">Request ID</span>: {item.orderId}
        <a href="" onClick={(e) => e.preventDefault()}> Cancel</a>
      </span>
        {attributeOrder.map(attrName => <span key={`${attrName}-${item.orderId}`} className="userAttribute"><span
          className="userAttributeName">{attrName}</span>: {item.userAttributes[attrName]}</span>)}
        <a href="" onClick={
          (e) => {
            e.preventDefault()
            this.context.handleSetOtherUserEmail(item.email)
            this.context.redirect({ to: '/edit-user' })
          }
        }>Update</a>
      </Card>
    )

    return (
      <tr>
        <td>{new Date(item.created).toUTCString()}</td>
        <td>
          <OverlayTrigger rootClose={true} trigger="click" placement="right" overlay={userAttributesPopover}>
            <a href="" onClick={(e) => e.preventDefault()}><BsInfoCircleFill/> {item.requester}</a>
          </OverlayTrigger>
        </td>
        <td>{item.institutionName}</td>
        <td>{item.email}</td>
        <td>{item.filter}</td>
        <td>{item.status}</td>
      </tr>)
  }
}

TransactionItem.contextType = AppContext

export default TransactionItem
