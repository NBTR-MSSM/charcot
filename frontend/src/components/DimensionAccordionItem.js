import { Component } from 'react'
import Accordion from 'react-bootstrap/Accordion'
import Category from './Category'
import { AppContext } from '../lib/context'

class DimensionAccordionItem extends Component {
  render () {
    const info = this.props.info
    return (<Accordion.Item bsPrefix='charcot-accordion-item' eventKey={this.props.eventKey}>
      <Accordion.Header>{info.displayName}</Accordion.Header>
      <Accordion.Body>
        {info.body || Array.from(info.categories.values()).map((category, index) => {
          return <Category key={index}
                           category={category}
                           dimension={info.dimension}/>
        })}
      </Accordion.Body>
    </Accordion.Item>)
  }
}

DimensionAccordionItem.contextType = AppContext

export default DimensionAccordionItem
