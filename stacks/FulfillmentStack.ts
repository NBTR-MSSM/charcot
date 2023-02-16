import * as sst from 'sst/constructs'
import { use } from 'sst/constructs'
import { fileURLToPath } from 'url'
import * as route53 from 'aws-cdk-lib/aws-route53'
import * as route53Targets from 'aws-cdk-lib/aws-route53-targets'
import { Duration } from 'aws-cdk-lib'
import { Certificate } from 'aws-cdk-lib/aws-certificatemanager'
import * as iam from 'aws-cdk-lib/aws-iam'
import * as sqs from 'aws-cdk-lib/aws-sqs'
import * as ecs from 'aws-cdk-lib/aws-ecs'
import { BackEndPaidAccountStack } from './BackEndPaidAccountStack'
import { CommonStack } from './CommonStack'
// eslint-disable-next-line camelcase
import ecs_patterns = require('aws-cdk-lib/aws-ecs-patterns')
import path = require('path')
import { Vpc } from 'aws-cdk-lib/aws-ec2'

export function FulfillmentStack({ stack }: sst.StackContext) {
  const {
    cerebrumImageOrderTableArn,
    cerebrumImageOrderQueueArn,
    cerebrumImageMetadataTableArn
  } = use(BackEndPaidAccountStack)
  const {
    zipBucketName,
    vpc
  } = use(CommonStack)

  const stage = stack.stage

  /*
   * When deploying the ODP account stack (BackEndOdpStack), SST/CDK still goes through all the stacks to init and synth
   * them, including this stack. This stack throws error when trying to lookup VPC in ODP account which was created in paid
   * account. Workaround is for CommonStack to set undefined 'vpc' for ODP account, and give preference to that undefined
   * VPC here just to keep SST/CDK init/synth process happy.
   */
  const cluster = new ecs.Cluster(stack, 'CharcotFulfillmentServiceCluster', {
    clusterName: `${stage}-charcot`,
    vpc: !vpc ? vpc : (process.env.VpcId ? Vpc.fromLookup(stack, 'VPC', { vpcId: process.env.VpcId }) : vpc)
  })

  const taskDefinition = new ecs.FargateTaskDefinition(stack, 'CharcotFulfillmentServiceTaskDefinition', {
    runtimePlatform: {
      operatingSystemFamily: ecs.OperatingSystemFamily.LINUX,
      cpuArchitecture: ecs.CpuArchitecture.ARM64
    },
    ephemeralStorageGiB: 200,
    cpu: 2048,
    memoryLimitMiB: 16384
  })

  const containerDefinition = new ecs.ContainerDefinition(stack, 'CharcotFulfillmentServiceContainerDefinition', {
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    image: ecs.ContainerImage.fromAsset(path.resolve(path.dirname(fileURLToPath(import.meta.url)), 'fulfillment'), {
      buildArgs: {
        STAGE: stage
      }
    }),
    taskDefinition,
    logging: new ecs.AwsLogDriver({
      streamPrefix: `${stage}-charcot-fulfillment`
    })
  })

  containerDefinition.addPortMappings({
    containerPort: 80
  })

  /*
   * Instantiate Fargate Service with a cluster and a local image that gets
   * uploaded to an S3 staging bucket prior to being uploaded to ECR.
   * A new repository is created in ECR and the Fargate service is created
   * with the image from ECR.
   */
  const service = new ecs_patterns.ApplicationLoadBalancedFargateService(stack, 'CharcotFulfillmentService', {
    taskDefinition,
    serviceName: `${stage}-charcot-fulfillment`,
    assignPublicIp: true, // TODO: Hide it from the world?
    certificate: Certificate.fromCertificateArn(stack, 'MyCert', 'arn:aws:acm:us-east-1:045387143127:certificate/1004f57f-a544-476d-8a31-5b878a71c276'),
    desiredCount: 1,
    cluster
  })

  const scalableTaskCount = service.service.autoScaleTaskCount({
    maxCapacity: 5,
    minCapacity: 1
  })

  const orderQueue = sqs.Queue.fromQueueArn(stack, 'orderQueue', cerebrumImageOrderQueueArn)
  /*
   * Keep 5 instances running as long as there are in-flight requests
   */
  scalableTaskCount.scaleOnMetric('fulfillmentScaleOutPolicy', {
    metric: orderQueue.metricApproximateNumberOfMessagesNotVisible(),
    // adjustmentType: AdjustmentType.EXACT_CAPACITY,
    scalingSteps: [
      {
        lower: 1,
        change: +4
      },
      {
        upper: 0,
        change: -4
      }
    ]
  })

  // Add policy statements so that ECS tasks can perform/carry out the pertinent actions
  const cerebrumImageOdpBucketNameProdStage = 'nbtr-production'
  service.taskDefinition.taskRole.addToPrincipalPolicy(new iam.PolicyStatement({
    effect: iam.Effect.ALLOW,
    actions: ['dynamodb:GetItem', 'dynamodb:UpdateItem'],
    resources: [cerebrumImageOrderTableArn]
  }))
  service.taskDefinition.taskRole.addToPrincipalPolicy(new iam.PolicyStatement({
    effect: iam.Effect.ALLOW,
    actions: ['sqs:ReceiveMessage', 'sqs:DeleteMessage'],
    resources: [cerebrumImageOrderQueueArn as string]
  }))
  service.taskDefinition.taskRole.addToPrincipalPolicy(new iam.PolicyStatement({
    effect: iam.Effect.ALLOW,
    actions: ['dynamodb:GetItem'],
    resources: [cerebrumImageMetadataTableArn]
  }))
  service.taskDefinition.taskRole.addToPrincipalPolicy(new iam.PolicyStatement({
    effect: iam.Effect.ALLOW,
    actions: ['s3:GetObject'],
    resources: [`arn:aws:s3:::${cerebrumImageOdpBucketNameProdStage}/*`]
  }))
  service.taskDefinition.taskRole.addToPrincipalPolicy(new iam.PolicyStatement({
    effect: iam.Effect.ALLOW,
    actions: ['s3:ListBucket'],
    resources: [`arn:aws:s3:::${cerebrumImageOdpBucketNameProdStage}`]
  }))
  service.taskDefinition.taskRole.addToPrincipalPolicy(new iam.PolicyStatement({
    effect: iam.Effect.ALLOW,
    actions: ['s3:PutObject', 's3:GetObject'],
    resources: [`arn:aws:s3:::${zipBucketName}/*`]
  }))
  service.taskDefinition.taskRole.addToPrincipalPolicy(new iam.PolicyStatement({
    effect: iam.Effect.ALLOW,
    actions: ['ses:SendEmail'],
    resources: ['*']
  }))

  service.targetGroup.configureHealthCheck({
    path: '/actuator/health'
  })

  // associate the ALB DNS name with a fixed domain
  const hostedZone = route53.HostedZone.fromHostedZoneAttributes(stack, 'HostedZone', {
    hostedZoneId: 'Z0341163303ASZWMW1YTS',
    zoneName: 'mountsinaicharcot.org'
  })

  // eslint-disable-next-line no-new
  new route53.ARecord(stack, 'charcot-fulfillment-dns-a-record', {
    recordName: stage === 'prod' ? 'fulfillment' : `fulfillment-${stage}`,
    zone: hostedZone,
    target: route53.RecordTarget.fromAlias(new route53Targets.LoadBalancerTarget(service.loadBalancer)),
    ttl: Duration.minutes(1)
  })

  stack.addOutputs({
    FulfillmentServiceTaskRoleArn: service.taskDefinition.taskRole.roleArn
  })
  return { fulfillmentServiceTaskRoleArn: service.taskDefinition.taskRole.roleArn }
}
