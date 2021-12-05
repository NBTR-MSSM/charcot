export interface CerebrumImageMetaData {
  fileName: string
  regionName: 'Orbital Frontal Cortex' | 'Orbital Frontal Cortex'
  stain: 'H&E' | 'Modified Beilschowski'
  age: string
  race: string
  sex: 'Male' | 'Female'
  uploadDate: string,
  imageNumber: number,
  total: number
}

export interface CerebrumImageMetaDataCreateResult {
  image: CerebrumImageMetaData,
  success: boolean,
  message: string
}
