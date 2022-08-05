import { Component } from 'react'
import { AppContext } from '../lib/context'
import './Splash.css'

class Splash extends Component {
  componentDidMount () {
    console.log('JMQ: mounted splash')
    this.context.pushToHistory()
  }

  render () {
    return (<div className='Splash'><h1 align='center'>The Mount Sinai and JJ Peters VA Medical Center MIRECC
      NIH Neurobiobank</h1>
      <ul className='links'>
        <li><a href='https://icahn.mssm.edu/research/nih-brain-tissue-repository'>The Mount Sinai Neurobiobank</a> - Who
          we are
        </li>
        <li><a href='/home'>Charcot</a> - The Neurobiobank Digital Neuropathology Slide Archive</li>
      </ul>
      <table>
        <tr className='.icon-container'>
          <td><span><img className='icon' src='./building.jpg'/></span></td>
          <td><span><img className='icon' src='./hospital.jpg'/></span></td>
          <td><span><img className='icon' src='./brain.jpg'/></span></td>
        </tr>
      </table>
      <table className='icon-container2'>
        <tr>
          <td><span className="square border-end"><img className='icon2 ismms' src='./ismms.jpg'/></span></td>
          <td><span className="square border-end"><img className='icon2 us-dept-va-affairs'
                                                       src='./us-dept-va-affairs.jpg'/>Mental Illness Research and Education Clinical Center (MIRECC)</span>
          </td>
          <td><span><img className='icon2' src='./neuro-bio-bank.jpg'/>The Mount Sinai NIH Neurobiobank</span></td>
        </tr>
      </table>
    </div>)
  }
}

Splash.contextType = AppContext

export default Splash
