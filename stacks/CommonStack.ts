import { IVpc, SubnetType, Vpc } from 'aws-cdk-lib/aws-ec2'
import { Construct } from 'constructs'
import * as sst from '@serverless-stack/resources'

export default class CommonStack extends sst.Stack {
  vpc: IVpc
  vpcId: string

  constructor(scope: Construct, id: string, props: sst.StackProps) {
    super(scope, id, props)

    this.vpc = new Vpc(this, 'CharcotFulfillmentServiceVpc', {
      vpcName: `${this.stage}-charcot`,
      cidr: '10.1.0.0/17',
      maxAzs: 2,
      subnetConfiguration: [
        {
          name: 'charcot-ingress',
          subnetType: SubnetType.PUBLIC
        }
      ]
    })

    this.vpcId = this.vpc.vpcId
    this.addOutputs({
      VpcId: this.vpcId
    })
  }
}
