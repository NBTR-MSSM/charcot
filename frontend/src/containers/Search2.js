import React, { Component } from 'react'
import { API } from 'aws-amplify'
import { Query, Builder, Utils as QbUtils } from 'react-awesome-query-builder'
import 'react-awesome-query-builder/lib/css/styles.css'
import 'react-awesome-query-builder/lib/css/compact_styles.css' // optional, for more compact styles
import ImageList from './ImageList'
// import InitialConfig from 'react-awesome-query-builder/lib/config/bootstrap'
import InitialConfig from 'react-awesome-query-builder/lib/config/mui'
// import InitialConfig from 'react-awesome-query-builder/lib/config/material'
// import InitialConfig from 'react-awesome-query-builder/lib/config/antd'

const config = {
  ...InitialConfig,
  fields: {
    region: {
      label: 'Region',
      type: 'select',
      valueSources: ['value'],
      fieldSettings: {
        asyncFetch: async (search, offset) => ({
          values: await API.get('charcot', '/cerebrum-images/regions', {}),
          hasMore: false
        })
      }
    },
    sex: {
      label: 'Sex',
      type: 'select',
      valueSources: ['value'],
      fieldSettings: {
        asyncFetch: async (search, offset) => ({
          values: await API.get('charcot', '/cerebrum-images/sexs', {}),
          hasMore: false
        })
      }
    },
    stain: {
      label: 'Stain',
      type: 'select',
      valueSources: ['value'],
      fieldSettings: {
        asyncFetch: async (search, offset) => ({
          values: await API.get('charcot', '/cerebrum-images/stains', {}),
          hasMore: false
        })
      }
    },
    age: {
      label: 'Age',
      type: 'number',
      valueSources: ['value'],
      fieldSettings: {
        min: 1,
        max: 130
      },
      preferWidgets: ['slider', 'rangeslider']
    },
    race: {
      label: 'Race',
      type: 'select',
      valueSources: ['value'],
      fieldSettings: {
        asyncFetch: async (search, offset) => ({
          values: await API.get('charcot', '/cerebrum-images/races', {}),
          hasMore: false
        })
      }
    }
  }
}

// You can load query value from your backend storage (for saving see `Query.onChange()`)
// const queryValue = savedQuery || { id: QbUtils.uuid(), type: 'group' }
const savedState = {
  query: { id: QbUtils.uuid(), type: 'group' }
}
export default class Search2 extends Component {
  constructor (props) {
    super(props)
    this.state = {
      tree: QbUtils.checkTree(QbUtils.loadTree(savedState.query), config),
      config: config
    }
  }

  async retrieveImages (query) {
    return await API.get('charcot', '/cerebrum-images?region=Orbital Frontal Cortex', {})
  }

  render = () => (
    <div>
      <Query
        {...config}
        value={this.state.tree}
        onChange={this.onChange}
        renderBuilder={this.renderBuilder}
      />
      <h4>Click anywhere on a row to toggle select:</h4>
      <ImageList images={this.props.images} onImageClick={this.props.onImageClick}/>
      {this.renderResult(this.state)}
    </div>
  )

  renderBuilder = (props) => (
    <div className="query-builder-container" style={{ padding: '10px' }}>
      <div className="query-builder qb-lite">
        <Builder {...props} />
      </div>
    </div>
  )

  renderResult = ({ tree: immutableTree, config }) => (
    <div className="query-builder-result">
      <b><i>Debugging info (will be removed before release):</i></b>
      <div>Query string: <pre>{JSON.stringify(QbUtils.queryString(immutableTree, config))}</pre></div>
      <div>MongoDb query: <pre>{JSON.stringify(QbUtils.mongodbFormat(immutableTree, config))}</pre></div>
      <div>SQL where: <pre>{JSON.stringify(QbUtils.sqlFormat(immutableTree, config))}</pre></div>
      <div>JsonLogic: <pre>{JSON.stringify(QbUtils.jsonLogicFormat(immutableTree, config))}</pre></div>
    </div>
  )

  onChange = async (immutableTree, config) => {
    // Tip: for better performance you can apply `throttle` - see `examples/demo`
    this.setState({ tree: immutableTree, config: config })
    savedState.query = QbUtils.getTree(immutableTree)
    this.props.onImageSearch(await this.retrieveImages(savedState.query))
  }
}
