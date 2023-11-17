import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client'

export const agesScanResult: DocumentClient.ScanOutput = {
  Items: [
    {
      age: 102
    },
    {
      age: 90
    },
    {
      age: 89
    },
    {
      age: 89
    },
    {
      age: 86
    },
    {
      age: 84
    },
    {
      age: 83
    },
    {
      age: 80
    },
    {
      age: 78
    },
    {
      age: 77
    },
    {
      age: 75
    },
    {
      age: 72
    },
    {
      age: 71
    },
    {
      age: 69
    },
    {
      age: 66
    },
    {
      age: 65
    },
    {
      age: 63
    },
    {
      age: 60
    },
    {
      age: 59
    },
    {
      age: 57
    },
    {
      age: 54
    },
    {
      age: 53
    },
    {
      age: 50
    },
    {
      age: 48
    },
    {
      age: 47
    },
    {
      age: 44
    },
    {
      age: 42
    },
    {
      age: 41
    },
    {
      age: 38
    },
    {
      age: 36
    },
    {
      age: 35
    },
    {
      age: 32
    },
    {
      age: 30
    },
    {
      age: 29
    },
    {
      age: 27
    },
    {
      age: 24
    },
    {
      age: 23
    },
    {
      age: 24
    },
    {
      age: 23
    },
    {
      age: 20
    },
    {
      age: 18
    },
    {
      age: 17
    },
    {
      age: 15
    },
    {
      age: 12
    },
    {
      age: 11
    },
    {
      age: 9
    },
    {
      age: 1
    }
  ]
}

export const agesPaginatedScanResult: DocumentClient.ScanOutput = {
  LastEvaluatedKey: {
    foo: 'bar'
  },
  Items: agesScanResult.Items
}

export const agesOutput = [
  {
    count: 1,
    title: 102,
    category: 102,
    range: '90+',
    rank: 102
  },
  {
    count: 1,
    title: 90,
    category: 90,
    range: '90+',
    rank: 90
  },
  {
    count: 2,
    title: 89,
    category: 89,
    range: '80 - 89',
    rank: 89
  },
  {
    count: 1,
    title: 86,
    category: 86,
    range: '80 - 89',
    rank: 86
  },
  {
    count: 1,
    title: 84,
    category: 84,
    range: '80 - 89',
    rank: 84
  },
  {
    count: 1,
    title: 83,
    category: 83,
    range: '80 - 89',
    rank: 83
  },
  {
    count: 1,
    title: 80,
    category: 80,
    range: '80 - 89',
    rank: 80
  },
  {
    count: 1,
    title: 78,
    category: 78,
    range: '70 - 79',
    rank: 78
  },
  {
    count: 1,
    title: 77,
    category: 77,
    range: '70 - 79',
    rank: 77
  },
  {
    count: 1,
    title: 75,
    category: 75,
    range: '70 - 79',
    rank: 75
  },
  {
    count: 1,
    title: 72,
    category: 72,
    range: '70 - 79',
    rank: 72
  },
  {
    count: 1,
    title: 71,
    category: 71,
    range: '70 - 79',
    rank: 71
  },
  {
    count: 1,
    title: 69,
    category: 69,
    range: '60 - 69',
    rank: 69
  },
  {
    count: 1,
    title: 66,
    category: 66,
    range: '60 - 69',
    rank: 66
  },
  {
    count: 1,
    title: 65,
    category: 65,
    range: '60 - 69',
    rank: 65
  },
  {
    count: 1,
    title: 63,
    category: 63,
    range: '60 - 69',
    rank: 63
  },
  {
    count: 1,
    title: 60,
    category: 60,
    range: '60 - 69',
    rank: 60
  },
  {
    count: 1,
    title: 59,
    category: 59,
    range: '50 - 59',
    rank: 59
  },
  {
    count: 1,
    title: 57,
    category: 57,
    range: '50 - 59',
    rank: 57
  },
  {
    count: 1,
    title: 54,
    category: 54,
    range: '50 - 59',
    rank: 54
  },
  {
    count: 1,
    title: 53,
    category: 53,
    range: '50 - 59',
    rank: 53
  },
  {
    count: 1,
    title: 50,
    category: 50,
    range: '50 - 59',
    rank: 50
  },
  {
    count: 1,
    title: 48,
    category: 48,
    range: '40 - 49',
    rank: 48
  },
  {
    count: 1,
    title: 47,
    category: 47,
    range: '40 - 49',
    rank: 47
  },
  {
    count: 1,
    title: 44,
    category: 44,
    range: '40 - 49',
    rank: 44
  },
  {
    count: 1,
    title: 42,
    category: 42,
    range: '40 - 49',
    rank: 42
  },
  {
    count: 1,
    title: 41,
    category: 41,
    range: '40 - 49',
    rank: 41
  },
  {
    count: 1,
    title: 38,
    category: 38,
    range: '30 - 39',
    rank: 38
  },
  {
    count: 1,
    title: 36,
    category: 36,
    range: '30 - 39',
    rank: 36
  },
  {
    count: 1,
    title: 35,
    category: 35,
    range: '30 - 39',
    rank: 35
  },
  {
    count: 1,
    title: 32,
    category: 32,
    range: '30 - 39',
    rank: 32
  },
  {
    count: 1,
    title: 30,
    category: 30,
    range: '30 - 39',
    rank: 30
  },
  {
    count: 1,
    title: 29,
    category: 29,
    range: '20 - 29',
    rank: 29
  },
  {
    count: 1,
    title: 27,
    category: 27,
    range: '20 - 29',
    rank: 27
  },
  {
    count: 2,
    title: 24,
    category: 24,
    range: '20 - 29',
    rank: 24
  },
  {
    count: 2,
    title: 23,
    category: 23,
    range: '20 - 29',
    rank: 23
  },
  {
    count: 1,
    title: 20,
    category: 20,
    range: '20 - 29',
    rank: 20
  },
  {
    count: 1,
    title: 18,
    category: 18,
    range: '10 - 19',
    rank: 18
  },
  {
    count: 1,
    title: 17,
    category: 17,
    range: '10 - 19',
    rank: 17
  },
  {
    count: 1,
    title: 15,
    category: 15,
    range: '10 - 19',
    rank: 15
  },
  {
    count: 1,
    title: 12,
    category: 12,
    range: '10 - 19',
    rank: 12
  },
  {
    count: 1,
    title: 11,
    category: 11,
    range: '10 - 19',
    rank: 11
  },
  {
    count: 1,
    title: 9,
    category: 9,
    range: '< 10',
    rank: 9
  },
  {
    count: 1,
    title: 1,
    category: 1,
    range: '< 10',
    rank: 1
  }
]

export const diagnosesScanResult: DocumentClient.ScanOutput = {
  Items: [
    {
      diagnosis: "Definite Alzheimer's Disease"
    },
    {
      diagnosis: 'Normal Brain'
    },
    {
      diagnosis: "Probable Alzheimer's Disease"
    },
    {
      diagnosis: "Lewy Body Dementia w/ Alzheimer's"
    },
    {
      diagnosis: 'Tumor'
    },
    {
      diagnosis: 'Other'
    },
    {
      diagnosis: 'Vascular Disease'
    },
    {
      diagnosis: "Possible Alzheimer's Disease"
    },
    {
      diagnosis: "Probable Alzheimer's Disease"
    },
    {
      diagnosis: 'unknown'
    },
    {
      diagnosis: 'Amyotropic Lateral Sclerosis (ALS)'
    },
    {
      diagnosis: 'Alcohol Dependency'
    },
    {
      diagnosis: 'Chronic Traumatic Encephalopathy (CTE)'
    },
    {
      diagnosis: 'Frontotemporal Dementia (FTD)'
    },
    {
      diagnosis: 'Major Depressive Disorder'
    },
    {
      diagnosis: 'Schizophrenia'
    },
    {
      diagnosis: "Lewy Body Dementia w/o Alzheimer's"
    },
    {
      diagnosis: 'Creutzfeldt-Jakob Disease (CJD)'
    },
    {
      diagnosis: "Pick's Disease"
    },
    {
      diagnosis: "Uncertain Parkinson's Disease"
    },
    {
      diagnosis: 'Other'
    },
    {
      diagnosis: 'Schizophrenia'
    },
    {
      diagnosis: 'Tumor'
    },
    {
      diagnosis: "Pick's Disease"
    },
    {
      diagnosis: "Uncertain Parkinson's Disease"
    },
    {
      diagnosis: 'Alcohol Dependency'
    },
    {
      diagnosis: "Probable Alzheimer's Disease"
    },
    {
      diagnosis: "Possible Alzheimer's Disease"
    },
    {
      diagnosis: "Definite Alzheimer's Disease"
    },
    {
      diagnosis: "Lewy Body Dementia w/o Alzheimer's"
    },
    {
      diagnosis: 'dummy'
    }
  ]
}

export const diagnosesOutput = [
  {
    count: 1,
    title: 'Normal Brain',
    category: 'normal-brain',
    rank: -1
  },
  {
    count: 2,
    title: "Definite Alzheimer's Disease",
    category: 'definite-alzheimer-s-disease',
    rank: -1
  },
  {
    count: 3,
    title: "Probable Alzheimer's Disease",
    category: 'probable-alzheimer-s-disease',
    rank: -1
  },
  {
    count: 2,
    title: "Possible Alzheimer's Disease",
    category: 'possible-alzheimer-s-disease',
    rank: -1
  },
  {
    count: 2,
    title: "Uncertain Parkinson's Disease",
    category: 'uncertain-parkinson-s-disease',
    rank: -1
  },
  {
    count: 1,
    title: 'Vascular Disease',
    category: 'vascular-disease',
    rank: -1
  },
  {
    count: 2,
    title: "Pick's Disease",
    category: 'pick-s-disease',
    rank: -1
  },
  {
    count: 1,
    title: 'Creutzfeldt-Jakob Disease (CJD)',
    category: 'creutzfeldt-jakob-disease-cjd',
    rank: -1
  },
  {
    count: 2,
    title: 'Tumor',
    category: 'tumor',
    rank: -1
  },
  {
    count: 2,
    title: "Lewy Body Dementia w/o Alzheimer's",
    category: 'lewy-body-dementia-w-o-alzheimer-s',
    rank: -1
  },
  {
    count: 1,
    title: "Lewy Body Dementia w/ Alzheimer's",
    category: 'lewy-body-dementia-w-alzheimer-s',
    rank: -1
  },
  {
    count: 2,
    title: 'Schizophrenia',
    category: 'schizophrenia',
    rank: -1
  },
  {
    count: 1,
    title: 'Major Depressive Disorder',
    category: 'major-depressive-disorder',
    rank: -1
  },
  {
    count: 1,
    title: 'Frontotemporal Dementia (FTD)',
    category: 'frontotemporal-dementia-ftd',
    rank: -1
  },
  {
    count: 1,
    title: 'Chronic Traumatic Encephalopathy (CTE)',
    category: 'chronic-traumatic-encephalopathy-cte',
    rank: -1
  },
  {
    count: 2,
    title: 'Alcohol Dependency',
    category: 'alcohol-dependency',
    rank: -1
  },
  {
    count: 1,
    title: 'Amyotropic Lateral Sclerosis (ALS)',
    category: 'amyotropic-lateral-sclerosis-als',
    rank: -1
  },
  {
    count: 2,
    title: 'Other',
    category: 'other',
    rank: -1
  },
  {
    count: 1,
    title: 'unknown',
    category: 'unknown',
    rank: -1
  },
  {
    count: 1,
    title: 'dummy',
    category: 'dummy',
    rank: -1
  }
]

export const allFieldsScanResult: DocumentClient.ScanOutput = {
  Items: [
    {
      subjectNumber: 31737,
      uploadDate: '06/03/2022',
      enabled: true,
      race: 'White (nonHispanic)',
      fileName: '31737_4_HE_1.mrxs',
      diagnosis: "Definite Alzheimer's Disease'",
      sex: 'Male',
      region: 'Basal Ganglia',
      stain: 'H&E',
      age: 82
    },
    {
      subjectNumber: 12345,
      uploadDate: '06/03/2022',
      enabled: true,
      race: 'White (nonHispanic)',
      fileName: '12345_4_HE_1.mrxs',
      diagnosis: "Definite Alzheimer's Disease'",
      sex: 'Male',
      region: 'Basal Ganglia',
      stain: 'H&E',
      age: 88
    },
    {
      subjectNumber: 67890,
      uploadDate: '06/03/2022',
      enabled: true,
      race: 'White (nonHispanic)',
      fileName: '67890_4_HE_1.mrxs',
      diagnosis: "Definite Alzheimer's Disease'",
      sex: 'Male',
      region: 'Basal Ganglia',
      stain: 'H&E',
      age: 83
    }
  ]
}

export const allFieldsScanResultPaginated: DocumentClient.ScanOutput = {
  Items: allFieldsScanResult.Items,
  LastEvaluatedKey: {
    foo: 'bar'
  }
}
