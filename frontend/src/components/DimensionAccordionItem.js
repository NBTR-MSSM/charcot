import { Component } from 'react'
import { Accordion } from 'react-bootstrap'
import Category from './Category'
import { AppContext } from '../lib/context'

class DimensionAccordionItem extends Component {
  render() {
    const info = this.props.info
    const dimension = info.dimension
    return (<Accordion.Item bsPrefix='charcot-accordion-item' eventKey={this.props.eventKey}>
      <Accordion.Header>{info.displayName}{this.props.displayCategoryPredicates ? <span className="categoryPredicates">{this.context.filter.serializeCategories(dimension).replace(new RegExp(`${dimension}\\s=\\s`, 'g'), '')}</span> : ''}</Accordion.Header>
      <Accordion.Body>
        {info.body || Array.from(info.categories.values()).map((category, index) => {
          return <Category key={index}
                           category={category}
                           dimension={dimension}/>
        })}
      </Accordion.Body>
    </Accordion.Item>)
  }
}

DimensionAccordionItem.contextType = AppContext

export default DimensionAccordionItem
