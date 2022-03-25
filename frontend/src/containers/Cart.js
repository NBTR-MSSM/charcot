import React, { Component } from 'react'
import './Cart.css'
import ImageList from './ImageList'

export default class Cart extends Component {
  render = () => (
    <div className='Cart'>
      <h3>Your Selections:</h3>
      <ImageList images={this.props.selectedImages} onImageClick={this.props.onImageClick} isCart={true}/>
    </div>
  )
}
