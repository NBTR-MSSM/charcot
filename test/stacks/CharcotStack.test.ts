import { expect, haveResource } from '@aws-cdk/assert'
import * as sst from '@serverless-stack/resources'
import CharcotStack from '../../stacks/CharcotStack'

test('Charcot Stack', () => {
  const app = new sst.App()
  // WHEN
  const stack = new CharcotStack(app, 'charcot-stack')
  // THEN
  expect(stack).to(haveResource('AWS::Lambda::Function'))
})
