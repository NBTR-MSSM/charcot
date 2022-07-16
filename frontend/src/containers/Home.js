import React, { Component } from 'react'
import './Home.css'
import { AppContext } from '../lib/context'

class Home extends Component {
  componentDidMount () {
    this.context.handleRouteLoad({
      active: 'home'
    })
  }

  render () {
    return (
      <div className="Home">
        <div className="lander">
          <p className="text-muted section main">Multi-stain, multiregional digital neuropathology slides of the human
            brain
            throughout the lifespan of persons with no brain disorders or neurologic and psychiatric conditions.
            Specimens
            are ethically received with the consent of the donors and/or their next-of-kin and receive a detailed
            neuropathology examination. The microscopic slides used for neuropathology are digitized at high resolution
            (20X) and presented here. Brain regions include middle frontal gyrus, cingulate gyrus, superior temporal
            gyrus, hippocampus, amygdala, basal ganglia, thalamus, etc. Histochemical and immunohistochemical stains
            include H&E, modified Bielschowski, amyloid beta, hyperphosphorylated tau, alpha synuclein, TDP-43. Brain
            disorders range from psychiatric conditions such as schizophrenia and major depression to Alzheimer's
            disease,
            Parkinson's disease, Lewy body disease and cerebrovascular disease. Some digital neuropathology slides are
            from donors who died by suicide. Although the collection includes digital neuropathology slides from persons
            spanning the entire human lifespan, the collection is concentrated on donors who died between the ages of
            65-90 years. In collaborative studies, the digital neuropathology slides (upon request and IRB approval) can
            be associated with a rich and large phenotypic dataset that include systemic and brain-related medical
            diagnoses during life (coded as ICD-10 codes), cognitive status during the peri-agonal stages of life,
            medications received during life, tissue toxicology and serology, antemortem laboratory test findings (blood
            glucose, HgA1C, , albumin, BUN, etc.), research-level assessment of meeting or failing to meet objective
            published criteria for clinical, neurological, and neuropsychiatric conditions, and objective
            criterion-based
            neuropathology diagnoses.</p>
          <h2 className="justify-content-end">How to use this dataset</h2>
          <p className="text-muted section">The available slides, which are updated roughly quarterly, are shown for
            specific demographic characteristics (age, race, sex), specific stains used (e.g., H&E, modified
            Bielschowski,
            amyloid beta, hyperphosphorylated tau), brain regions (e.g., middle frontal gyrus, hippocampus), and primary
            neuropathology diagnosis. Note that primary neuropathology diagnosis DOES NOT necessarily EXCLUDE the
            presence
            of co-morbid secondary neuropathologies. More detailed diagnostic information for most donors can be made
            available through collaborative agreements. Tissues, often in snap-frozen and formalin-fixed form, are
            available for the donors and can be accessed through the NIH-Neurobiobank (https://neurobiobank.nih.gov).
          </p>
          <h2>Registration</h2>
          <p className="text-muted section">Although we are making these slide images freely and publicly available,
            before selecting slide images and downloading, we ask you to register with us. We ask this because for our
            internal purposes, record keeping, and reporting we would like to know who you are, how you intend to use
            the
            images, and what scientific questions you intend to address.
          </p>
          <h2>Credits</h2>
          <p className="text-muted section">
            The actual generation of digital slide images has been supported in part by the Leir Foundation in Memory of
            Henry J. and Erna D. Leir, Founders. Our laboratories have been supported by multiple grants from the NIH
            for
            studies of aging, Alzheimer’s disease, Parkinson’s disease, and mental health. We have also been gratefully
            supported by the Department of Veterans Affairs through multiple grants and its Mental Illness Research
            Education and Clinical Center at the JJ Peters VAMC. The brain banking and donor phenotyping represents over
            35 years of collaboration by many many scientists, including V. Haroutunian, D. Purohit, P. Roussos, K.L.
            Davis, M. Signaevski and many more.
          </p>
          <h2>Disclaimer</h2>
          <p className="text-muted section">
            With over 40,000 images from over 1,900 donors, some mislabeling, mismatches and errors are inevitable. The
            digital neuropathology slides are presented as-is with the understanding that some errors will exist.
          </p>
        </div>
      </div>
    )
  }
}

Home.contextType = AppContext

export default Home
