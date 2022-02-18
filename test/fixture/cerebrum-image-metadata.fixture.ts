import { CerebrumImageMetaData } from '../../src/types/charcot.types'

export default [
  {
    fileName: 'XE13-009_2_HE_1.mrxs',
    region: 'Orbital Frontal Cortex',
    stain: 'H&E',
    age: '53',
    race: 'White (nonHispanic)',
    sex: 'Male',
    uploadDate: '3/17/2020',
    imageNumber: 1,
    total: 3
  },
  {
    fileName: 'XE13-009_2_Sil_1.mrxs',
    region: 'Orbital Frontal Cortex',
    stain: 'Modified Beilschowski',
    age: '53',
    race: 'White (nonHispanic)',
    sex: 'Male',
    uploadDate: '3/17/2020',
    imageNumber: 2,
    total: 3
  },
  {
    fileName: 'XE12-025_1_HE_1.mrxs',
    region: 'Middle Frontal Gyrus',
    stain: 'H&E',
    age: '91',
    race: 'White (nonHispanic)',
    sex: 'Female',
    uploadDate: '3/17/2020',
    imageNumber: 3,
    total: 3
  }
] as Readonly<Array<Readonly<CerebrumImageMetaData>>>
