import { Component } from 'react'
import Form from 'react-bootstrap/Form'

export default class Category extends Component {
  constructor (props) {
    super(props)
    this.state = { checked: false }
  }

  async componentDidMount () {
    this.updateState({ checked: this.props.category.selected })
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
    const { checked, value } = event.target
    const [dimension, category] = value.split('|')
    if (checked) {
      this.props.onCategorySelect({ dimension, category })
    } else {
      this.props.onCategoryUnselect({ dimension, category })
    }
  }

  render () {
    const { category, dimension } = this.props
    return (
      <Form>
        <div key={category.name} className="mb-3">
          <Form.Check
            checked={this.state.checked}
            type='checkbox'
            label={category.name}
            onChange={this.handleCategoryChange}
            value={`${dimension}|${category.name}`}
            disabled={category.count < 1}
          />
        </div>
      </Form>
    )
  }
}
