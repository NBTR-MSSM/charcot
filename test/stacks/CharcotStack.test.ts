import { Template } from 'aws-cdk-lib/assertions'
import * as sst from '@serverless-stack/resources'
import CharcotStack from '../../stacks/CharcotStack'

test('Charcot Stack', () => {
  const app = new sst.App()
  // WHEN
  const stack = new CharcotStack(app, 'charcot-stack')

  const template = Template.fromStack(stack)

  // THEN
  template.resourceCountIs('AWS::Lambda::Function', 1)
})
