export interface CerebellumImageMetaData {
  fileName: string
  regionName: 'Orbital Frontal Cortex' | 'Orbital Frontal Cortex'
  stain: 'H&E' | 'Modified Beilschowski'
  age: string
  race: string
  sex: 'Male' | 'Female'
  uploadDate: string
}

export interface CerebellumImageMetaDataCreateResult {
  image: CerebellumImageMetaData,
  success: boolean,
  message: string
}
