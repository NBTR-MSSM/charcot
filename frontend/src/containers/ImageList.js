import { Component } from 'react'
import ImageListItem from './ImageListItem'
import './ImageList.css'

export default class ImageList extends Component {
  render () {
    const rows = this.props.images.map((image) => {
      return <ImageListItem image={image} isSelected={image.isSelected}
                            onImageClick={this.props.onImageClick} isCart={this.props.isCart}/>
    })
    return (
      <div className="ImageList">
        <table>
          <thead>
          <tr key="header">
            <td><b>File Name</b></td>
            <td><b>Region</b></td>
            <td><b>Stain</b></td>
            <td><b>Age</b></td>
            <td><b>Race</b></td>
            <td><b>Sex</b></td>
            {this.props.isCart && <td></td>}
          </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
      </div>)
  }
}
