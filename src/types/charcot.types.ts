type CharcotFileName = string

export type Filter = string

export interface CerebrumImageMetaData {
  fileName: CharcotFileName
  region: string
  stain: string
  age: string
  race: string
  sex: 'Male' | 'Female'
  uploadDate: string
  imageNumber: number
  total: number
}

export interface CerebrumImageOrder {
  [key: string]: string | number | Array<string> | Record<string, string> | boolean | undefined
  orderId: string
  recordNumber: number
  created: number
  fileNames: CharcotFileName[]
  filesProcessed?: CharcotFileName[]
  filter?: Filter
  email: string
  status: 'received' | 'processing' | 'processed' | 'canceled' | 'cancel-requested'
  remark?: string
  size?: number
  isCancellable?: boolean
}

export interface Pagination {
  pageSize: number,
  totalPages: number,
  page: number
}

export interface OrderTotals {
  orderCount: number,
  size: number,
  slides: number,
  uniqueUsers: number
}

export interface OrderRetrievalOutput extends OrderTotals, Pagination {
  orders: CerebrumImageOrder[]
}

export type Range = string

export interface Dimension {
  category: string | number
  title: string
  count: number
  range: Range | undefined
  rank: number
}

export interface RangeInfo {
  range: Range
  rank: number
}
