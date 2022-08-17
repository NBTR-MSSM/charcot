import { IVpc, SubnetFilter, SubnetSelection, SubnetType, Vpc } from 'aws-cdk-lib/aws-ec2'
import { Construct } from 'constructs'
import * as sst from '@serverless-stack/resources'

export default class CommonStack extends sst.Stack {
  vpc: IVpc
  publicSubnets: SubnetSelection
  privateSubnets: SubnetSelection
  vpcId: string

  constructor(scope: Construct, id: string, props: sst.StackProps) {
    super(scope, id, props)

    const stage = this.stage

    this.vpc = new Vpc(this, 'CharcotFulfillmentServiceVpc', {
      vpcName: `${stage}-charcot`,
      maxAzs: 2,
      subnetConfiguration: [
        {
          name: 'charcot-application',
          subnetType: SubnetType.PRIVATE_WITH_NAT
        },
        {
          name: 'charcot-ingress',
          subnetType: SubnetType.PUBLIC
        }
      ]
    })

    const privateSubnetsList = this.vpc.privateSubnets.map(e => e.subnetId)
    const publicSubnetsList = this.vpc.publicSubnets.map(e => e.subnetId)
    this.privateSubnets = {
      subnetFilters: [SubnetFilter.byIds(privateSubnetsList)]
    }
    this.publicSubnets = {
      subnetFilters: [SubnetFilter.byIds(publicSubnetsList)]
    }

    this.vpcId = this.vpc.vpcId
    this.addOutputs({
      PrivateSubnets: privateSubnetsList.join(','),
      PublicSubnets: publicSubnetsList.join(','),
      VpcId: this.vpcId
    })
  }
}
