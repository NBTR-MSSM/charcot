import { Component } from 'react'
import Accordion from 'react-bootstrap/Accordion'
import Category from './Category'

export default class DimensionAccordionItem extends Component {
  render () {
    const info = this.props.info
    return (<Accordion.Item bsPrefix='charcot-accordion-item' eventKey={this.props.eventKey}>
      <Accordion.Header>{info.displayName}</Accordion.Header>
      <Accordion.Body>
        {Array.from(info.categories.values()).map((category, index) => {
          return <Category key={index}
                           category={category}
                           dimension={info.dimension}
                           onCategorySelect={this.props.onCategorySelect}
                           onCategoryUnselect={this.props.onCategoryUnselect}/>
        })}
      </Accordion.Body>
    </Accordion.Item>)
  }
}
