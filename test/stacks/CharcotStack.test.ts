import { Template } from 'aws-cdk-lib/assertions'
import * as sst from '@serverless-stack/resources'
import BackEndPaidAccountStack from '../../stacks/BackEndPaidAccountStack'

test('Charcot Stack', () => {
  const app = new sst.App()
  // WHEN
  const stack = new BackEndPaidAccountStack(app, 'charcot-stack', {}, {})

  const template = Template.fromStack(stack)

  // THEN
  template.resourceCountIs('AWS::Lambda::Function', 1)
})
