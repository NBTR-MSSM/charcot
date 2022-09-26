import * as sst from '@serverless-stack/resources'
import { fileURLToPath } from 'url'
import * as route53 from 'aws-cdk-lib/aws-route53'
import * as route53Targets from 'aws-cdk-lib/aws-route53-targets'
import { Duration } from 'aws-cdk-lib'
import { Certificate } from 'aws-cdk-lib/aws-certificatemanager'
import { StackArguments } from '../src/types/charcot.types'
import * as iam from 'aws-cdk-lib/aws-iam'
import ecs = require('aws-cdk-lib/aws-ecs')
import ecs_patterns = require('aws-cdk-lib/aws-ecs-patterns')
import path = require('path')

export default class FulfillmentStack extends sst.Stack {
  fulfillmentServiceTaskRoleArn: string

  constructor(scope: sst.App, id: string, props: sst.StackProps, args: StackArguments) {
    super(scope, id, props)

    const stage = this.stage

    const cluster = new ecs.Cluster(this, 'CharcotFulfillmentServiceCluster', {
      clusterName: `${stage}-charcot`,
      vpc: args.vpc
    })

    const taskDefinition = new ecs.FargateTaskDefinition(this, 'CharcotFulfillmentServiceTaskDefinition', {
      ephemeralStorageGiB: 200,
      cpu: 4096,
      memoryLimitMiB: 30720
    })

    const containerDefinition = new ecs.ContainerDefinition(this, 'CharcotFulfillmentServiceContainerDefinition', {
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
      image: ecs.ContainerImage.fromAsset(path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../fulfillment'), {
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
    const service = new ecs_patterns.ApplicationLoadBalancedFargateService(this, 'CharcotFulfillmentService', {
      taskDefinition,
      serviceName: `${stage}-charcot-fulfillment`,
      assignPublicIp: true, // TODO: Hide it from the world?
      certificate: Certificate.fromCertificateArn(this, 'MyCert', 'arn:aws:acm:us-east-1:045387143127:certificate/1004f57f-a544-476d-8a31-5b878a71c276'),
      cluster,
      desiredCount: 5
    })

    // Add policy statements so that ECS tasks can perform/carry out the pertinent actions
    const cerebrumImageOdpBucketNameProdStage = process.env.CEREBRUM_IMAGE_ODP_BUCKET_NAME
    service.taskDefinition.taskRole.addToPrincipalPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['dynamodb:GetItem'],
      resources: [args.cerebrumImageOrderTableArn as string]
    }))
    service.taskDefinition.taskRole.addToPrincipalPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['dynamodb:GetItem'],
      resources: [args.cerebrumImageMetadataTableArn as string]
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
      resources: [`arn:aws:s3:::${args.zipBucketName}/*`]
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
    const hostedZone = route53.HostedZone.fromHostedZoneAttributes(this, 'HostedZone', {
      hostedZoneId: 'Z0341163303ASZWMW1YTS',
      zoneName: 'mountsinaicharcot.org'
    })

    // eslint-disable-next-line no-new
    new route53.ARecord(this, 'charcot-fulfillment-dns-a-record', {
      recordName: stage === 'prod' ? 'fulfillment' : `fulfillment-${stage}`,
      zone: hostedZone,
      target: route53.RecordTarget.fromAlias(new route53Targets.LoadBalancerTarget(service.loadBalancer)),
      ttl: Duration.minutes(1)
    })

    this.fulfillmentServiceTaskRoleArn = service.taskDefinition.taskRole.roleArn
    this.addOutputs({
      FulfillmentServiceTaskRoleArn: this.fulfillmentServiceTaskRoleArn
    })
  }
}
