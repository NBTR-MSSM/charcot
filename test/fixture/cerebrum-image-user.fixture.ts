import { CognitoIdentityServiceProvider } from 'aws-sdk'
import merge from 'lodash.merge'

function createCopy<Type>(source: Type): Type {
  const copy = {} as Type
  merge(copy, source)
  return copy
}

export const userFactory = () => {
  return createCopy(user)
}

export const updateRequestFactory = () => user.UserAttributes!.reduce((accum, current) => {
  accum[current.Name] = current.Value
  return accum
}, {} as Record<string, string | undefined>)

const user: CognitoIdentityServiceProvider.AdminGetUserResponse = {
  Username: 'b02712c7-ff3b-4b3d-975a-c1b91a408519',
  UserAttributes: [
    {
      Name: 'custom:institutionAddress',
      Value: '123 Main St\nAmherst, MA 01002'
    },
    {
      Name: 'sub',
      Value: 'b02712c7-ff3b-4b3d-975a-c1b91a408519'
    },
    {
      Name: 'email_verified',
      Value: 'true'
    },
    {
      Name: 'custom:institutionName',
      Value: 'University of Massachusetts'
    },
    {
      Name: 'custom:areasOfInterest',
      Value: 'Research'
    },
    {
      Name: 'custom:intendedUse',
      Value: 'Test desc'
    },
    {
      Name: 'given_name',
      Value: 'Jose'
    },
    {
      Name: 'custom:degree',
      Value: 'BS'
    },
    {
      Name: 'family_name',
      Value: 'Quijada'
    },
    {
      Name: 'email',
      Value: 'john.doe@gmail.com'
    }
  ],
  UserCreateDate: new Date('2022-07-17T11:42:15.556000-04:00'),
  UserLastModifiedDate: new Date('2022-10-28T11:46:12.457000-04:00'),
  Enabled: true,
  UserStatus: 'CONFIRMED'
}
