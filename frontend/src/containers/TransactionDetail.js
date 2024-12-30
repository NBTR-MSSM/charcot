import React, { Component } from 'react'
import { Table } from 'react-bootstrap'
import Button from 'react-bootstrap/Button'
import './TransactionDetail.css'
import { AppContext } from '../lib/context'
import BootstrapTable from 'react-bootstrap-table-next'
import {
  handleOrderApproval,
  moreInfoNeededModal,
  handleMoreInfoNeeded,
  orderApprovalConfirmationModal,
  successfulOrderApprovalConfirmationModal, successfulMoreInfoNeededModal
} from '../lib/orderApproval'

const persistedState = {
  // isShowSuccessfulOrderApprovalConfirmation: false
}

const columns = [{
  dataField: 'field',
  text: 'Field'
}, {
  dataField: 'value',
  text: 'Value'
}]

class TransactionDetail extends Component {
  constructor(props) {
    super(props)
    this.moreInfoNeededFormRef = React.createRef()
    this.state = {
      // isShowOrderApprovalConfirmationModal: false,
      // isShowSuccessfulOrderApprovalConfirmation: false,
      // isShowOrderCancelConfirmationModal: false,
      // isShowMoreInfoNeededModal: false,
      infoNeeded: ''
    }
  }

  async componentDidMount() {
    this.context.pushToHistory()
  }

  render() {
    const item = this.context.transactionItem
    const remarks = [...item.remark.matchAll(/\[[^[]+/g)]
    const expandRow = {
      renderer: () => {
        return <Table striped bordered hover>
          <tbody>
          {remarks.map((e, idx) => {
            return <tr key={idx}>
              <td className="filler"></td>
              <td>{e}</td>
            </tr>
          })}
          </tbody>
        </Table>
      },
      showExpandColumn: true,
      nonExpandable: ['Request Date', 'Requester', 'Institution', 'Email', 'Criteria', 'Size', 'Slide Count', 'Status', 'Intended Use']
    }
    const info = [
      {
        field: 'Request Date',
        value: new Date(item.created).toUTCString()
      },
      {
        field: 'Requester',
        value: item.requester
      },
      {
        field: 'Institution',
        value: item.institutionName
      },
      {
        field: 'Email',
        value: item.email
      },
      {
        field: 'Criteria',
        value: item.filter
      },
      {
        field: 'Size',
        value: `${Number.parseFloat(item.size / Math.pow(2, 30)).toFixed(2)}GB`
      },
      {
        field: 'Slide Count',
        value: item.fileCount
      },
      {
        field: 'Status',
        value: item.status
      },
      {
        field: 'Intended Use',
        value: item.intendedUse
      },
      {
        field: 'Remarks',
        value: ''
      }]
    const setStateFunction = this.setState.bind(this)
    return <div className="TransactionDetail">
      {orderApprovalConfirmationModal(this, persistedState, this.context.email, item.orderId)}
      {successfulOrderApprovalConfirmationModal(item, this, persistedState)}
      {moreInfoNeededModal(this)}
      {successfulMoreInfoNeededModal(item, this, persistedState)}
      <Button id="back-btn" size="sm"
              onClick={() => this.context.redirect({ to: '/transaction' })}>{'< Back'}</Button>
      {item.isApprovable
        ? <>
          <Button variant="warning" id="more-info-needed-btn" size="sm"
                  onClick={(event) => handleMoreInfoNeeded(event, setStateFunction)}>{'Request More Info'}</Button>
          <Button id="approve-btn" size="sm"
                  onClick={(event) => handleOrderApproval(event, setStateFunction)}>{'Approve'}</Button></>
        : ''}
      {/* From https://react-bootstrap-table.github.io/react-bootstrap-table2/docs/getting-started.html */}
      <BootstrapTable keyField="field"
                      data={info}
                      columns={columns}
                      expandRow={expandRow}
                      showExpandColumn={true}/>
    </div>
  }
}

TransactionDetail.contextType = AppContext

export default TransactionDetail
