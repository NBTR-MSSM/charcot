import { SubnetType, Vpc } from 'aws-cdk-lib/aws-ec2'
import * as sst from 'sst/constructs'

const calculateZipBucketName = (stage: string) => {
  const bucketSuffix = stage === 'prod' ? '' : `-${stage}`
  return `cerebrum-image-zip${bucketSuffix}`
}

export function CommonStack({ stack }: sst.StackContext) {
  const zipBucketName = calculateZipBucketName(stack.stage)

  /*
   * VPC is irrelevant for ODP account. See FulfillmentStack for implications and reason
   * we do this for the ODP account.
   */
  if (stack.account === '950869325006') {
    return { zipBucketName, vpc: undefined }
  }
  const vpc = new Vpc(stack, 'CharcotFulfillmentServiceVpc', {
    vpcName: `${stack.stage}-charcot`,
    // ipAddresses: '',
    cidr: '10.1.0.0/17',
    maxAzs: 2,
    subnetConfiguration: [
      {
        name: 'charcot-ingress',
        subnetType: SubnetType.PUBLIC
      }
    ]
  })

  stack.addOutputs({
    VpcId: vpc.vpcId
  })

  return {
    vpcId: vpc.vpcId,
    zipBucketName,
    vpc
  }
}
