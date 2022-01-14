import CharcotStack from './CharcotStack'
import * as sst from '@serverless-stack/resources'
import CharcotStackOdp from './CharcotStackOdp'

export default function main(app: sst.App): void {
  // Set default runtime for all functions
  app.setDefaultFunctionProps({
    runtime: 'nodejs14.x'
  })

  // eslint-disable-next-line no-new
  new CharcotStack(app, 'charcot-stack')
  // eslint-disable-next-line no-new
  new CharcotStackOdp(app, 'charcot-stack-odp')
}
