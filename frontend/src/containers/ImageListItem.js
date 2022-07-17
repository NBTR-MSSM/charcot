import { Component } from 'react'
import './ImageListItem.css'

export default class ImageListItem extends Component {
  /**
   * Overloaded to handle toggle select from search page
   * and remove button from cart
   * TODO: Maybe just make everything button operated across the board to keep things consistent
   */
  handleClick = (e) => {
    let fileName
    if (e.currentTarget.tagName === 'TR') {
      fileName = e.currentTarget.childNodes[0].innerText
    } else {
      fileName = e.target.parentElement.parentElement.childNodes[0].innerText
    }
    this.props.onImageClick(fileName)
  }

  render () {
    const image = this.props.image
    let clickHandler = this.handleClick
    if (this.props.isCart) {
      clickHandler = (e) => e
    }

    return (
      <tr onClick={clickHandler} key={image.fileName}
          className={`ImageListItem ${this.props.isSelected && !this.props.isCart ? 'selected' : ''}`}>
        <td>{image.fileName}</td>
        <td>{image.region}</td>
        <td>{image.stain}</td>
        <td>{image.age}</td>
        <td>{image.race}</td>
        <td>{image.sex}</td>
        {this.props.isCart && <td>
          <button onClick={this.handleClick}>Remove</button>
        </td>}
      </tr>
    )
  }
}
