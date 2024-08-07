import { Component } from 'react'
import Form from 'react-bootstrap/Form'
import { AppContext } from '../lib/context'

class Category extends Component {
  constructor (props) {
    super(props)
    const {
      category,
      dimension
    } = this.props
    this.state = {
      checked: false,
      value: `${dimension}|${category.name}`
    }
  }

  async componentDidMount () {
    this.updateState({
      checked: this.props.category.selected
    })
  }

  componentDidUpdate (prevProps) {
    if (this.props.category.selected !== prevProps.category.selected) {
      this.updateState({ checked: this.props.category.selected })
    }
  }

  updateState = ({ checked }) => {
    this.setState({
      checked
    })
  }

  handleCategoryChange = (event) => {
    const {
      checked,
      value
    } = event.target
    const [dimension, category] = value.split('|')
    if (checked) {
      this.context.handleCategorySelect({
        dimension,
        category
      })
    } else {
      this.context.handleCategoryUnselect({
        dimension,
        category
      })
    }
  }

  render () {
    const {
      category,
      dimension
    } = this.props
    return (
      <div key={`${dimension}-${category.name}`} className="mb-3">
        <Form.Check
          checked={this.state.checked}
          type='checkbox'
          label={category.name}
          onChange={this.handleCategoryChange}
          value={this.state.value}
          disabled={category.count < 1}
        />
      </div>
    )
  }
}

Category.contextType = AppContext

export default Category
