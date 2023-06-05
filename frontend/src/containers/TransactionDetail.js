import React, { Component } from 'react'
import { Table } from 'react-bootstrap'
import Button from 'react-bootstrap/Button'
import './TransactionDetail.css'
import { AppContext } from '../lib/context'
import BootstrapTable from 'react-bootstrap-table-next'

const columns = [{
  dataField: 'field',
  text: 'Field'
}, {
  dataField: 'value',
  text: 'Value'
}]

class TransactionDetail extends Component {
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
      nonExpandable: ['Request Date', 'Requester', 'Institution', 'Email', 'Criteria', 'Size', 'Slide Count', 'Status']
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
        field: 'Remarks',
        value: ''
      }]
    return <div className="TransactionDetail">
      <Button id="back-btn" size="sm"
              onClick={() => this.context.redirectToPrevious()}>{'< Back'}</Button>
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
