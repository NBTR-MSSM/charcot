import { CerebrumImageOrder, OrderRetrievalOutput } from '../../src/types/charcot.types'
import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client'
import merge from 'lodash.merge'

function createCopy<Type>(source: Type): Type {
  const copy = {} as Type
  merge(copy, source)
  return copy
}

function obtainOrders(include: number[]) {
  const foundOrders = []
  for (const created of include) {
    foundOrders.push(createCopy(orders.filter(e => e.created === created)[0]))
  }
  return foundOrders
}

function produceOrderOutputWithHydratedOrders() {
  const orderOutputCopy = createCopy(orderOutput)
  // Merge each order into the corresponding retrieval output one
  orderOutputCopy.orders = orderOutputCopy.orders.map(e => {
    const orderId = e.orderId
    const order = orders.filter(e => e.orderId === orderId)[0]
    return { ...e, ...order }
  }) as unknown as CerebrumImageOrder[]
  return orderOutputCopy
}

export const orderOutputFactory = (include: number[] | undefined = undefined) => {
  const orderOutputCopy = produceOrderOutputWithHydratedOrders()

  if (include !== undefined) {
    orderOutputCopy.orders = obtainOrders(include)
  }
  return orderOutputCopy
}

/**
 * Sorts orders according to the createdOrder array argument, which contains created timestamps
 * in the same order that requests are to be stored in the returned OrderRetrievalOutput
 */
export const sortedOrderOutput = (createdOrder: number[]): OrderRetrievalOutput => {
  const orderOutputCopy = produceOrderOutputWithHydratedOrders()
  const orders = []
  for (const created of createdOrder) {
    orders.push(orderOutputCopy.orders.filter(e => e.created === created)[0])
  }
  orderOutputCopy.orders = orders
  return orderOutputCopy
}

const orders: CerebrumImageOrder[] = [
  {
    orderId: 'abc123',
    recordNumber: 0,
    email: 'clark.ken@acme.com',
    fileNames: ['XE13-009_2_HE_1.mrxs', 'XE13-009_2_Sil_1.mrxs'],
    filesProcessed: ['XE13-009_2_HE_1.mrxs', 'XE13-009_2_Sil_1.mrxs'],
    created: 1,
    size: 2048,
    status: 'received'
  },
  {
    orderId: 'def123',
    recordNumber: 0,
    email: 'peter.parker@acme.com',
    fileNames: ['XE13-009_2_HE_1.mrxs', 'XE13-009_2_Sil_1.mrxs', 'XE12-025_1_HE_1.mrxs'],
    filesProcessed: ['XE13-009_2_HE_1.mrxs', 'XE13-009_2_Sil_1.mrxs', 'XE12-025_1_HE_1.mrxs'],
    created: 2,
    size: 3072,
    status: 'received'
  },
  {
    orderId: 'ghi123',
    recordNumber: 0,
    email: 'bruce.wayne@acme.com',
    fileNames: ['XE13-009_2_HE_1.mrxs'],
    filesProcessed: ['XE13-009_2_HE_1.mrxs'],
    created: 3,
    size: 1024,
    status: 'received'
  },
  {
    orderId: 'jkl123',
    recordNumber: 0,
    email: 'alan.scott@acme.com',
    fileNames: ['XE13-009_2_HE_1.mrxs'],
    filesProcessed: ['XE13-009_2_HE_1.mrxs'],
    created: 4,
    size: 1024,
    status: 'received'
  },
  {
    orderId: 'mno123',
    recordNumber: 0,
    email: 'alan.scott@acme.com',
    fileNames: ['XE13-009_2_HE_1.mrxs'],
    filesProcessed: ['XE13-009_2_HE_1.mrxs'],
    created: 5,
    size: 1024,
    status: 'received'
  }
]

export const orderScanResultFactory = (include: number[] | undefined = undefined, single = false): DocumentClient.ScanOutput | DocumentClient.GetItemOutput => {
  if (single) {
    return {
      Item: obtainOrders(include!)[0]
    }
  } else {
    return {
      Items: include !== undefined ? obtainOrders(include) : orders
    }
  }
}

const dummyOrder: CerebrumImageOrder = {
  orderId: '',
  recordNumber: 0,
  email: '',
  fileNames: [],
  filesProcessed: [],
  created: -1,
  size: 0,
  status: 'received'
}

// Inject dummyOrder into each order of the array to keep TS compiler happy
const orderOutput: OrderRetrievalOutput = {
  orderCount: 5,
  pageSize: 10,
  totalPages: 1,
  page: -1,
  size: 8192,
  slides: 8,
  uniqueUsers: 4,
  orders: [
    {
      ...dummyOrder,
      orderId: 'mno123',
      institutionName: 'University of Massachusetts',
      family_name: 'Quijada',
      requester: 'Jose Quijada',
      userAttributes: {
        institutionAddress: '123 Main St\nAmherst, MA 01002',
        firstName: 'Jose',
        lastName: 'Quijada',
        sub: 'b02712c7-ff3b-4b3d-975a-c1b91a408519',
        email_verified: 'true',
        institutionName: 'University of Massachusetts',
        areasOfInterest: 'Research',
        intendedUse: 'Test desc',
        given_name: 'Jose',
        degree: 'BS',
        family_name: 'Quijada',
        email: 'john.doe@gmail.com',
        requester: 'Jose Quijada'
      },
      isCancellable: true
    },
    {
      ...dummyOrder,
      orderId: 'jkl123',
      institutionName: 'University of Massachusetts',
      family_name: 'Quijada',
      requester: 'Jose Quijada',
      userAttributes: {
        institutionAddress: '123 Main St\nAmherst, MA 01002',
        firstName: 'Jose',
        lastName: 'Quijada',
        sub: 'b02712c7-ff3b-4b3d-975a-c1b91a408519',
        email_verified: 'true',
        institutionName: 'University of Massachusetts',
        areasOfInterest: 'Research',
        intendedUse: 'Test desc',
        given_name: 'Jose',
        degree: 'BS',
        family_name: 'Quijada',
        email: 'john.doe@gmail.com',
        requester: 'Jose Quijada'
      },
      isCancellable: true
    },
    {
      ...dummyOrder,
      orderId: 'ghi123',
      institutionName: 'University of Massachusetts',
      family_name: 'Quijada',
      requester: 'Jose Quijada',
      userAttributes: {
        institutionAddress: '123 Main St\nAmherst, MA 01002',
        firstName: 'Jose',
        lastName: 'Quijada',
        sub: 'b02712c7-ff3b-4b3d-975a-c1b91a408519',
        email_verified: 'true',
        institutionName: 'University of Massachusetts',
        areasOfInterest: 'Research',
        intendedUse: 'Test desc',
        given_name: 'Jose',
        degree: 'BS',
        family_name: 'Quijada',
        email: 'john.doe@gmail.com',
        requester: 'Jose Quijada'
      },
      isCancellable: true
    },
    {
      ...dummyOrder,
      orderId: 'def123',
      institutionName: 'University of Massachusetts',
      family_name: 'Quijada',
      requester: 'Jose Quijada',
      userAttributes: {
        institutionAddress: '123 Main St\nAmherst, MA 01002',
        firstName: 'Jose',
        lastName: 'Quijada',
        sub: 'b02712c7-ff3b-4b3d-975a-c1b91a408519',
        email_verified: 'true',
        institutionName: 'University of Massachusetts',
        areasOfInterest: 'Research',
        intendedUse: 'Test desc',
        given_name: 'Jose',
        degree: 'BS',
        family_name: 'Quijada',
        email: 'john.doe@gmail.com',
        requester: 'Jose Quijada'
      },
      isCancellable: true
    },
    {
      ...dummyOrder,
      orderId: 'abc123',
      institutionName: 'University of Massachusetts',
      family_name: 'Quijada',
      requester: 'Jose Quijada',
      userAttributes: {
        institutionAddress: '123 Main St\nAmherst, MA 01002',
        firstName: 'Jose',
        lastName: 'Quijada',
        sub: 'b02712c7-ff3b-4b3d-975a-c1b91a408519',
        email_verified: 'true',
        institutionName: 'University of Massachusetts',
        areasOfInterest: 'Research',
        intendedUse: 'Test desc',
        given_name: 'Jose',
        degree: 'BS',
        family_name: 'Quijada',
        email: 'john.doe@gmail.com',
        requester: 'Jose Quijada'
      },
      isCancellable: true
    }
  ]
}
