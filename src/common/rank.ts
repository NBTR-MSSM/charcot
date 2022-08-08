const LOWEST_RANK = 99999
const rankings: Record<string, Record<string, number>> = {
  stain: {
    'H&E': 1,
    'Amyloid Beta': 2,
    'Modified Beilschowski': 3,
    'Phosphorylated Tau': 4, // should be combined with Tau
    LFB: 5, // should be combined with LFB-CV and LFB-PAS
    Synuclein: 6,
    Ubiquitin: 7,
    'TDP-43': 8,
    GFAP: 9,
    "Holzer's": 10,
    "Perl's Fe": 11,
    'SV40-IHC': 12,
    'LFB-CV': LOWEST_RANK,
    Tau: LOWEST_RANK,
    'LFB-PAS': LOWEST_RANK,
    TG3: LOWEST_RANK // Should be combined with phosphorylated tau
  },
  region: {
    'Middle Frontal Gyrus': 1,
    'Orbital frontal gyrus': 2,
    'Insular Cortex': 3,
    'Cingulate Gyrus': 4,
    'Middle Temporal Gyrus': 5,
    'Superior Temporal Gyrus': 6,
    Hippocampus: 7,
    'Inferior Parietal Lobule': 8,
    'Occipital Cortex': 9,
    'Basal Forebrain': 10,
    'Basal Ganglia': 11,
    Amygdala: 12,
    Thalamus: 13,
    Midbrain: 14,
    Pons: 15,
    Medulla: 16,
    Cerebellum: 17
  },
  diagnosis: {
    'Normal Brain': 11,
    "Definite Alzheimer's Disease": 12,
    "Probable Alzheimer's Disease": 13,
    "Possible Alzheimer's Disease": 14,
    "Definite Parkinson's Disease": 15,
    "Uncertain Parkinson's Disease": 16,
    'Vascular Disease': 17,
    "Pick's Disease": 18,
    'Creutzfeldt-Jakob Disease (CJD)': 20,
    Tumor: 24,
    "Lewy Body Dementia w/o Alzheimer's": 27,
    "Lewy Body Dementia w/ Alzheimer's": 28,
    Schizophrenia: 31,
    'Major Depressive Disorder': 32,
    'Frontotemporal Dementia (FTD)': 35,
    'Chronic Traumatic Encephalopathy (CTE)': 46,
    'Alcohol Dependency': 48,
    'Amyotropic Lateral Sclerosis (ALS)': 52,
    unkown: LOWEST_RANK,
    Other: LOWEST_RANK
  },
  race: {},
  sex: {}
}

export const rank = (dimension: string, category: string): number => {
  return rankings[dimension][category] || LOWEST_RANK
}
