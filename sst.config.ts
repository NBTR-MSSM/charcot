import { SSTConfig } from 'sst'
import { CommonStack } from './stacks/CommonStack'
import { BackEndPaidAccountStack } from './stacks/BackEndPaidAccountStack'
import { BackEndOdpStack } from './stacks/BackEndOdpStack'
import { FrontendStack } from './stacks/FrontEndStack'
import { FulfillmentStack } from './stacks/FulfillmentStack'

// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
export default {
  config() {
    return {
      name: 'charcot',
      region: 'us-east-1'
    }
  },
  stacks(app) {
    const stage = app.stage
    app.setDefaultFunctionProps({
      runtime: 'nodejs18.x'
    })
    app
      .stack(CommonStack, {
        id: 'common',
        stackName: `${stage}-${app.name}-common`,
        tags: { created_by: 'sst' }
      })
      .stack(BackEndPaidAccountStack, {
        id: 'backend-paid-account',
        stackName: `${stage}-${app.name}-backend-paid-account`,
        tags: { created_by: 'sst' }
      })

    // FIXME: FulfillmentStack creation times out SST 'dev' mode,
    //  fix and then remove this "if()"
    if (app.mode !== 'dev') {
      app.stack(FulfillmentStack, {
        id: 'fulfillment',
        stackName: `${stage}-${app.name}-fulfillment`,
        tags: { created_by: 'sst' }
      }).stack(BackEndOdpStack, {
        id: 'backend-odp',
        stackName: `${stage}-${app.name}-backend-odp`,
        tags: { created_by: 'sst' }
      })
    }

    app.stack(FrontendStack, {
      id: 'frontend',
      stackName: `${stage}-${app.name}-frontend`,
      tags: { created_by: 'sst' }
    })
  }
} satisfies SSTConfig
