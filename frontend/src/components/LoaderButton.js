import React, { Component } from 'react'
import Button from 'react-bootstrap/Button'
import { BsArrowRepeat } from 'react-icons/bs'
import './LoaderButton.css'

export default class LoaderButton extends Component {
  render () {
    const {
      isLoading,
      className = '',
      disabled = false,
      ...props
    } = this.props

    return (
      <Button
        disabled={disabled || isLoading}
        className={`LoaderButton ${className}`}
        {...props}>
        {isLoading && <BsArrowRepeat className='spinning'/>}
        {props.children}
      </Button>
    )
  }
}
