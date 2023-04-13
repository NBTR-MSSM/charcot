import { createRequire as topLevelCreateRequire } from 'module';const require = topLevelCreateRequire(import.meta.url);
var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });
var __require = /* @__PURE__ */ ((x) => typeof require !== "undefined" ? require : typeof Proxy !== "undefined" ? new Proxy(x, {
  get: (a, b) => (typeof require !== "undefined" ? require : a)[b]
}) : x)(function(x) {
  if (typeof require !== "undefined")
    return require.apply(this, arguments);
  throw new Error('Dynamic require of "' + x + '" is not supported');
});

// stacks/CommonStack.ts
import { SubnetType, Vpc } from "aws-cdk-lib/aws-ec2";
var calculateZipBucketName = /* @__PURE__ */ __name((stage) => {
  const bucketSuffix = stage === "prod" ? "" : `-${stage}`;
  return `cerebrum-image-zip${bucketSuffix}`;
}, "calculateZipBucketName");
function CommonStack({ stack }) {
  const zipBucketName = calculateZipBucketName(stack.stage);
  if (stack.account === "950869325006") {
    return { zipBucketName, vpc: void 0 };
  }
  const vpc = new Vpc(stack, "CharcotFulfillmentServiceVpc", {
    vpcName: `${stack.stage}-charcot`,
    cidr: "10.1.0.0/17",
    maxAzs: 2,
    subnetConfiguration: [
      {
        name: "charcot-ingress",
        subnetType: SubnetType.PUBLIC
      }
    ]
  });
  stack.addOutputs({
    VpcId: vpc.vpcId
  });
  return {
    vpcId: vpc.vpcId,
    zipBucketName,
    vpc
  };
}
__name(CommonStack, "CommonStack");

// stacks/BackEndPaidAccountStack.ts
import * as sst from "sst/constructs";
import * as iam from "aws-cdk-lib/aws-iam";
import { Bucket as S3Bucket, EventType } from "aws-cdk-lib/aws-s3";
import * as s3Notifications from "aws-cdk-lib/aws-s3-notifications";
import { StringAttribute } from "aws-cdk-lib/aws-cognito";
import { Certificate } from "aws-cdk-lib/aws-certificatemanager";
import * as route53 from "aws-cdk-lib/aws-route53";
import { Duration } from "aws-cdk-lib";
function BackEndPaidAccountStack({ stack }) {
  const stage = stack.stage;
  const auth = cognitoUserPool(stack);
  const cerebrumImageOrderQueue = new sst.Queue(stack, "cerebrum-image-order-queue", {
    cdk: {
      queue: {
        visibilityTimeout: Duration.hours(12),
        receiveMessageWaitTime: Duration.seconds(20)
      }
    }
  });
  const cerebrumImageMetaDataTable = new sst.Table(stack, "cerebrum-image-metadata", {
    fields: {
      fileName: "string",
      region: "string",
      stain: "string",
      age: "number",
      race: "string",
      sex: "string",
      diagnosis: "string",
      subjectNumber: "number",
      uploadDate: "string",
      enabled: "string"
    },
    primaryIndex: { partitionKey: "fileName" },
    globalIndexes: {
      regionIndex: { partitionKey: "region" },
      stainIndex: { partitionKey: "stain" },
      ageIndex: { partitionKey: "age" },
      raceIndex: { partitionKey: "race" },
      sexIndex: { partitionKey: "sex" },
      diagnosisIndex: { partitionKey: "diagnosis" },
      subjectNumberIndex: { partitionKey: "subjectNumber" }
    }
  });
  const cerebrumImageOrderTable = new sst.Table(stack, "cerebrum-image-order", {
    fields: {
      orderId: "string",
      email: "string",
      created: "number",
      filter: "string",
      status: "string",
      fulfilled: "number",
      remark: "string",
      sqsReceiptHandle: "string",
      size: "number",
      fileCount: "number"
    },
    primaryIndex: { partitionKey: "orderId" },
    globalIndexes: {
      createdIndex: { partitionKey: "created" }
    }
  });
  const bucketSuffix = stage === "prod" ? "" : `-${stage}`;
  const cerebrumImageBucketName = `nbtr-odp-staging${bucketSuffix}`;
  const cerebrumImageOdpBucketName = `nbtr-production${bucketSuffix}`;
  const handleCerebrumImageTransfer = new sst.Function(stack, "HandleCerebrumImageTransfer", {
    functionName: `handle-cerebrum-image-transfer-${stage}`,
    handler: "src/lambda/cerebrum-image-transfer.handle",
    memorySize: 128,
    initialPolicy: [
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["s3:GetObject", "s3:DeleteObject"],
        resources: [`arn:aws:s3:::${cerebrumImageBucketName}/*`]
      }),
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["s3:PutObject"],
        resources: [`arn:aws:s3:::${cerebrumImageOdpBucketName}/*`]
      })
    ],
    environment: {
      CEREBRUM_IMAGE_ODP_BUCKET_NAME: cerebrumImageOdpBucketName
    },
    timeout: 900
  });
  if (stage === "prod") {
    const loadedBucket = S3Bucket.fromBucketName(stack, "BucketLoadedByName", cerebrumImageBucketName);
    loadedBucket.addEventNotification(EventType.OBJECT_CREATED, new s3Notifications.LambdaDestination(handleCerebrumImageTransfer));
  } else {
    const cerebrumImageBucket = new sst.Bucket(stack, cerebrumImageBucketName, {
      name: cerebrumImageBucketName,
      notifications: {
        myNotification: {
          type: "function",
          function: handleCerebrumImageTransfer,
          events: ["object_created"]
        }
      }
    });
    cerebrumImageBucket.attachPermissions(["s3"]);
  }
  const hostedZone = route53.HostedZone.fromHostedZoneAttributes(stack, "HostedZone", {
    hostedZoneId: "Z0341163303ASZWMW1YTS",
    zoneName: "mountsinaicharcot.org"
  });
  const api = new sst.Api(stack, "Api", {
    customDomain: {
      domainName: `${stage === "prod" ? "api.mountsinaicharcot.org" : `api-${stage}.mountsinaicharcot.org`}`,
      cdk: {
        hostedZone,
        certificate: Certificate.fromCertificateArn(stack, "MyCert", "arn:aws:acm:us-east-1:045387143127:certificate/1004f57f-a544-476d-8a31-5b878a71c276")
      }
    },
    routes: {
      "POST /cerebrum-images": {
        function: {
          functionName: `create-cerebrum-image-metadata-${stage}`,
          handler: "src/lambda/cerebrum-image-metadata.create",
          initialPolicy: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ["dynamodb:PutItem"],
              resources: [cerebrumImageMetaDataTable.tableArn]
            })
          ],
          environment: {
            CEREBRUM_IMAGE_METADATA_TABLE_NAME: cerebrumImageMetaDataTable.tableName
          }
        }
      },
      "GET /cerebrum-images": {
        function: {
          functionName: `handle-cerebrum-image-search-${stage}`,
          handler: "src/lambda/cerebrum-image-search.search",
          initialPolicy: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ["dynamodb:Query", "dynamodb:Scan"],
              resources: [cerebrumImageMetaDataTable.tableArn, `${cerebrumImageMetaDataTable.tableArn}/index/*`]
            })
          ],
          environment: {
            CEREBRUM_IMAGE_METADATA_TABLE_NAME: cerebrumImageMetaDataTable.tableName
          }
        }
      },
      "GET /cerebrum-images/{dimension}": {
        function: {
          functionName: `handle-cerebrum-image-dimension-${stage}`,
          handler: "src/lambda/cerebrum-image-search.dimension",
          initialPolicy: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ["dynamodb:Query", "dynamodb:Scan"],
              resources: [cerebrumImageMetaDataTable.tableArn, `${cerebrumImageMetaDataTable.tableArn}/index/*`]
            })
          ],
          environment: {
            CEREBRUM_IMAGE_METADATA_TABLE_NAME: cerebrumImageMetaDataTable.tableName
          }
        }
      },
      "GET /cerebrum-image-users/{email}": {
        function: {
          functionName: `retrieve-cerebrum-image-user-${stage}`,
          handler: "src/lambda/cerebrum-image-user.retrieve",
          initialPolicy: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ["cognito-idp:AdminGetUser"],
              resources: [auth.userPoolArn]
            })
          ],
          environment: {
            CEREBRUM_COGNITO_USER_POOL_ID: auth.userPoolId
          }
        }
      },
      "PUT /cerebrum-image-users/{email}": {
        function: {
          functionName: `update-cerebrum-image-user-${stage}`,
          handler: "src/lambda/cerebrum-image-user.update",
          initialPolicy: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ["cognito-idp:AdminUpdateUserAttributes", "cognito-idp:AdminSetUserPassword"],
              resources: [auth.userPoolArn]
            })
          ],
          environment: {
            CEREBRUM_COGNITO_USER_POOL_ID: auth.userPoolId
          }
        }
      },
      "GET /cerebrum-image-orders": {
        function: {
          functionName: `retrieve-cerebrum-image-order-${stage}`,
          handler: "src/lambda/cerebrum-image-order.retrieve",
          initialPolicy: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ["dynamodb:Scan"],
              resources: [cerebrumImageOrderTable.tableArn]
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ["cognito-idp:AdminGetUser"],
              resources: [auth.userPoolArn]
            })
          ],
          environment: {
            CEREBRUM_IMAGE_ORDER_TABLE_NAME: cerebrumImageOrderTable.tableName,
            CEREBRUM_COGNITO_USER_POOL_ID: auth.userPoolId
          }
        }
      },
      "POST /cerebrum-image-orders": {
        authorizer: "iam",
        function: {
          functionName: `create-cerebrum-image-order-${stage}`,
          handler: "src/lambda/cerebrum-image-order.create",
          initialPolicy: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ["dynamodb:PutItem"],
              resources: [cerebrumImageOrderTable.tableArn]
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ["dynamodb:Query", "dynamodb:Scan"],
              resources: [cerebrumImageMetaDataTable.tableArn]
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ["sqs:SendMessage"],
              resources: [cerebrumImageOrderQueue.queueArn]
            })
          ],
          environment: {
            CEREBRUM_IMAGE_ORDER_TABLE_NAME: cerebrumImageOrderTable.tableName,
            CEREBRUM_IMAGE_ORDER_QUEUE_URL: cerebrumImageOrderQueue.queueUrl,
            CEREBRUM_IMAGE_METADATA_TABLE_NAME: cerebrumImageMetaDataTable.tableName
          }
        }
      },
      "DELETE /cerebrum-image-orders/{orderId}": {
        function: {
          functionName: `cancel-cerebrum-image-order-${stage}`,
          handler: "src/lambda/cerebrum-image-order.cancel",
          initialPolicy: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ["dynamodb:UpdateItem", "dynamodb:GetItem"],
              resources: [cerebrumImageOrderTable.tableArn]
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ["cognito-idp:AdminGetUser"],
              resources: [auth.userPoolArn]
            })
          ],
          environment: {
            CEREBRUM_IMAGE_ORDER_TABLE_NAME: cerebrumImageOrderTable.tableName,
            CEREBRUM_COGNITO_USER_POOL_ID: auth.userPoolId
          }
        }
      }
    }
  });
  auth.attachPermissionsForAuthUsers(stack, [api]);
  stack.addOutputs({
    ApiEndpoint: api.customDomainUrl || api.url,
    Region: stack.region,
    UserPoolId: auth.userPoolId,
    CognitoIdentityPoolId: auth.cognitoIdentityPoolId,
    UserPoolClientId: auth.userPoolClientId,
    HandleCerebrumImageTransferRoleArn: handleCerebrumImageTransfer?.role?.roleArn,
    CerebrumImageOrderTableArn: cerebrumImageOrderTable.tableArn,
    CerebrumImageMetadataTableArn: cerebrumImageMetaDataTable.tableArn,
    CerebrumImageOrderQueueArn: cerebrumImageOrderQueue.queueArn,
    CerebrumImageOrderQueueUrl: cerebrumImageOrderQueue.queueUrl,
    CerebrumImageOrderQueueName: cerebrumImageOrderQueue.queueName
  });
  return {
    api,
    handleCerebrumImageTransferRoleArn: handleCerebrumImageTransfer?.role?.roleArn,
    userPoolId: auth.userPoolId,
    userPoolClientId: auth.userPoolClientId,
    cognitoIdentityPoolId: auth.cognitoIdentityPoolId,
    cerebrumImageOrderTableArn: cerebrumImageOrderTable.tableArn,
    cerebrumImageMetadataTableArn: cerebrumImageMetaDataTable.tableArn,
    cerebrumImageOrderQueueArn: cerebrumImageOrderQueue.queueArn
  };
}
__name(BackEndPaidAccountStack, "BackEndPaidAccountStack");
var cognitoUserPool = /* @__PURE__ */ __name((scope) => new sst.Cognito(scope, "Auth", {
  login: ["email"],
  cdk: {
    userPool: {
      customAttributes: {
        degree: new StringAttribute({
          minLen: 1,
          maxLen: 256,
          mutable: true
        }),
        institutionName: new StringAttribute({
          minLen: 1,
          maxLen: 256,
          mutable: true
        }),
        institutionAddress: new StringAttribute({
          minLen: 1,
          maxLen: 256,
          mutable: true
        }),
        areasOfInterest: new StringAttribute({
          minLen: 1,
          maxLen: 256,
          mutable: true
        }),
        intendedUse: new StringAttribute({
          minLen: 1,
          maxLen: 500,
          mutable: true
        })
      }
    }
  }
}), "cognitoUserPool");

// stacks/BackEndOdpStack.ts
import * as sst2 from "sst/constructs";
import * as iam3 from "aws-cdk-lib/aws-iam";
import { Bucket as S3Bucket2 } from "aws-cdk-lib/aws-s3";
import { use as use2 } from "sst/constructs";

// stacks/FulfillmentStack.ts
import { use } from "sst/constructs";
import { fileURLToPath } from "url";
import * as route532 from "aws-cdk-lib/aws-route53";
import * as route53Targets from "aws-cdk-lib/aws-route53-targets";
import { Duration as Duration2 } from "aws-cdk-lib";
import { Certificate as Certificate2 } from "aws-cdk-lib/aws-certificatemanager";
import * as iam2 from "aws-cdk-lib/aws-iam";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as ecs from "aws-cdk-lib/aws-ecs";
import { Vpc as Vpc2 } from "aws-cdk-lib/aws-ec2";
var ecs_patterns = __require("aws-cdk-lib/aws-ecs-patterns");
var path = __require("path");
function FulfillmentStack({ stack }) {
  const {
    cerebrumImageOrderTableArn,
    cerebrumImageOrderQueueArn,
    cerebrumImageMetadataTableArn
  } = use(BackEndPaidAccountStack);
  const {
    zipBucketName,
    vpc
  } = use(CommonStack);
  const stage = stack.stage;
  const cluster = new ecs.Cluster(stack, "CharcotFulfillmentServiceCluster", {
    clusterName: `${stage}-charcot`,
    vpc: !vpc ? vpc : process.env.VpcId ? Vpc2.fromLookup(stack, "VPC", { vpcId: process.env.VpcId }) : vpc
  });
  const taskDefinition = new ecs.FargateTaskDefinition(stack, "CharcotFulfillmentServiceTaskDefinition", {
    runtimePlatform: {
      operatingSystemFamily: ecs.OperatingSystemFamily.LINUX,
      cpuArchitecture: ecs.CpuArchitecture.ARM64
    },
    ephemeralStorageGiB: 200,
    cpu: 2048,
    memoryLimitMiB: 16384
  });
  const containerDefinition = new ecs.ContainerDefinition(stack, "CharcotFulfillmentServiceContainerDefinition", {
    image: ecs.ContainerImage.fromAsset(path.resolve(path.dirname(fileURLToPath(import.meta.url)), "fulfillment"), {
      buildArgs: {
        STAGE: stage
      }
    }),
    taskDefinition,
    logging: new ecs.AwsLogDriver({
      streamPrefix: `${stage}-charcot-fulfillment`
    })
  });
  containerDefinition.addPortMappings({
    containerPort: 80
  });
  const service = new ecs_patterns.ApplicationLoadBalancedFargateService(stack, "CharcotFulfillmentService", {
    taskDefinition,
    serviceName: `${stage}-charcot-fulfillment`,
    assignPublicIp: true,
    certificate: Certificate2.fromCertificateArn(stack, "MyCert", "arn:aws:acm:us-east-1:045387143127:certificate/1004f57f-a544-476d-8a31-5b878a71c276"),
    desiredCount: 1,
    cluster
  });
  const scalableTaskCount = service.service.autoScaleTaskCount({
    maxCapacity: 5,
    minCapacity: 1
  });
  const orderQueue = sqs.Queue.fromQueueArn(stack, "orderQueue", cerebrumImageOrderQueueArn);
  scalableTaskCount.scaleOnMetric("fulfillmentScaleOutPolicy", {
    metric: orderQueue.metricApproximateNumberOfMessagesNotVisible(),
    scalingSteps: [
      {
        lower: 1,
        change: 4
      },
      {
        upper: 0,
        change: -4
      }
    ]
  });
  const cerebrumImageOdpBucketNameProdStage = "nbtr-production";
  service.taskDefinition.taskRole.addToPrincipalPolicy(new iam2.PolicyStatement({
    effect: iam2.Effect.ALLOW,
    actions: ["dynamodb:GetItem", "dynamodb:UpdateItem"],
    resources: [cerebrumImageOrderTableArn]
  }));
  service.taskDefinition.taskRole.addToPrincipalPolicy(new iam2.PolicyStatement({
    effect: iam2.Effect.ALLOW,
    actions: ["sqs:ReceiveMessage", "sqs:DeleteMessage"],
    resources: [cerebrumImageOrderQueueArn]
  }));
  service.taskDefinition.taskRole.addToPrincipalPolicy(new iam2.PolicyStatement({
    effect: iam2.Effect.ALLOW,
    actions: ["dynamodb:GetItem"],
    resources: [cerebrumImageMetadataTableArn]
  }));
  service.taskDefinition.taskRole.addToPrincipalPolicy(new iam2.PolicyStatement({
    effect: iam2.Effect.ALLOW,
    actions: ["s3:GetObject"],
    resources: [`arn:aws:s3:::${cerebrumImageOdpBucketNameProdStage}/*`]
  }));
  service.taskDefinition.taskRole.addToPrincipalPolicy(new iam2.PolicyStatement({
    effect: iam2.Effect.ALLOW,
    actions: ["s3:ListBucket"],
    resources: [`arn:aws:s3:::${cerebrumImageOdpBucketNameProdStage}`]
  }));
  service.taskDefinition.taskRole.addToPrincipalPolicy(new iam2.PolicyStatement({
    effect: iam2.Effect.ALLOW,
    actions: ["s3:PutObject", "s3:GetObject"],
    resources: [`arn:aws:s3:::${zipBucketName}/*`]
  }));
  service.taskDefinition.taskRole.addToPrincipalPolicy(new iam2.PolicyStatement({
    effect: iam2.Effect.ALLOW,
    actions: ["ses:SendEmail"],
    resources: ["*"]
  }));
  service.targetGroup.configureHealthCheck({
    path: "/actuator/health"
  });
  const hostedZone = route532.HostedZone.fromHostedZoneAttributes(stack, "HostedZone", {
    hostedZoneId: "Z0341163303ASZWMW1YTS",
    zoneName: "mountsinaicharcot.org"
  });
  new route532.ARecord(stack, "charcot-fulfillment-dns-a-record", {
    recordName: stage === "prod" ? "fulfillment" : `fulfillment-${stage}`,
    zone: hostedZone,
    target: route532.RecordTarget.fromAlias(new route53Targets.LoadBalancerTarget(service.loadBalancer)),
    ttl: Duration2.minutes(1)
  });
  stack.addOutputs({
    FulfillmentServiceTaskRoleArn: service.taskDefinition.taskRole.roleArn
  });
  return { fulfillmentServiceTaskRoleArn: service.taskDefinition.taskRole.roleArn };
}
__name(FulfillmentStack, "FulfillmentStack");

// stacks/BackEndOdpStack.ts
function BackEndOdpStack({ stack }) {
  const { zipBucketName } = use2(CommonStack);
  let { fulfillmentServiceTaskRoleArn } = use2(FulfillmentStack);
  let { handleCerebrumImageTransferRoleArn } = use2(BackEndPaidAccountStack);
  fulfillmentServiceTaskRoleArn = process.env.FulfillmentServiceTaskRoleArn || fulfillmentServiceTaskRoleArn;
  handleCerebrumImageTransferRoleArn = process.env.HandleCerebrumImageTransferRoleArn || handleCerebrumImageTransferRoleArn;
  const stage = stack.stage;
  const bucketSuffix = stage === "prod" ? "" : `-${stage}`;
  const cerebrumImageOdpBucketName = `nbtr-production${bucketSuffix}`;
  const cerebrumImageZipBucketName = zipBucketName;
  let cerebrumImageOdpBucket;
  if (stage === "prod") {
    cerebrumImageOdpBucket = S3Bucket2.fromBucketName(stack, "ODPBucketLoadedByName", cerebrumImageOdpBucketName);
  } else {
    cerebrumImageOdpBucket = new sst2.Bucket(stack, cerebrumImageOdpBucketName, {
      name: cerebrumImageOdpBucketName
    }).cdk.bucket;
  }
  cerebrumImageOdpBucket.addToResourcePolicy(
    new iam3.PolicyStatement({
      effect: iam3.Effect.ALLOW,
      principals: [new iam3.ArnPrincipal(handleCerebrumImageTransferRoleArn)],
      actions: ["s3:PutObject"],
      resources: [`${cerebrumImageOdpBucket.bucketArn}/*`]
    })
  );
  const cerebrumImageZipBucket = new sst2.Bucket(stack, cerebrumImageZipBucketName, {
    name: cerebrumImageZipBucketName
  });
  cerebrumImageZipBucket.cdk.bucket.addToResourcePolicy(
    new iam3.PolicyStatement({
      effect: iam3.Effect.ALLOW,
      principals: [new iam3.ArnPrincipal(fulfillmentServiceTaskRoleArn)],
      actions: ["s3:GetObject", "s3:PutObject"],
      resources: [`${cerebrumImageZipBucket.bucketArn}/*`]
    })
  );
  cerebrumImageOdpBucket.addToResourcePolicy(
    new iam3.PolicyStatement({
      sid: "Allow Charcot Fulfillment Service to Read Objects",
      effect: iam3.Effect.ALLOW,
      principals: [new iam3.ArnPrincipal(fulfillmentServiceTaskRoleArn)],
      actions: ["s3:GetObject"],
      resources: [`${cerebrumImageOdpBucket.bucketArn}/*`]
    })
  );
  cerebrumImageOdpBucket.addToResourcePolicy(
    new iam3.PolicyStatement({
      sid: "Allow Charcot Fulfillment Service to List Objects",
      effect: iam3.Effect.ALLOW,
      principals: [new iam3.ArnPrincipal(fulfillmentServiceTaskRoleArn)],
      actions: ["s3:ListBucket"],
      resources: [`${cerebrumImageOdpBucket.bucketArn}`]
    })
  );
  stack.addOutputs({
    CerebrumImageOdpBucketName: cerebrumImageOdpBucketName
  });
}
__name(BackEndOdpStack, "BackEndOdpStack");

// stacks/FrontEndStack.ts
import * as sst3 from "sst/constructs";
import { Certificate as Certificate3 } from "aws-cdk-lib/aws-certificatemanager";
import * as route533 from "aws-cdk-lib/aws-route53";
import { use as use3 } from "sst/constructs";
function FrontendStack({ stack }) {
  const {
    api,
    userPoolId,
    userPoolClientId,
    cognitoIdentityPoolId
  } = use3(BackEndPaidAccountStack);
  const environment = {
    REACT_APP_API_URL: process.env.ApiEndpoint || api.customDomainUrl || api.url,
    REACT_APP_REGION: stack.region,
    REACT_APP_USER_POOL_ID: userPoolId,
    REACT_APP_USER_POOL_CLIENT_ID: userPoolClientId,
    REACT_APP_IDENTITY_POOL_ID: cognitoIdentityPoolId
  };
  const stage = stack.stage;
  const hostedZone = route533.HostedZone.fromHostedZoneAttributes(stack, "HostedZone", {
    hostedZoneId: "Z0341163303ASZWMW1YTS",
    zoneName: "mountsinaicharcot.org"
  });
  const site = new sst3.StaticSite(stack, "ReactSite", {
    path: "frontend",
    buildCommand: "npm run build",
    buildOutput: "build",
    environment,
    customDomain: {
      domainName: stage === "prod" ? "www.mountsinaicharcot.org" : `${stage}.mountsinaicharcot.org`,
      domainAlias: stage === "prod" ? "mountsinaicharcot.org" : void 0,
      cdk: {
        hostedZone,
        certificate: Certificate3.fromCertificateArn(stack, "MyCert", "arn:aws:acm:us-east-1:045387143127:certificate/1004f57f-a544-476d-8a31-5b878a71c276")
      }
    }
  });
  if (stack.stage !== "debug") {
    stack.addOutputs({
      SiteUrl: site.customDomainUrl || site.url,
      DistributionDomain: site.cdk.distribution.distributionDomainName,
      DistributionId: site.cdk.distribution.distributionId,
      Environment: JSON.stringify(environment, null, 2)
    });
  } else {
    stack.addOutputs({
      SiteUrl: "foo"
    });
  }
}
__name(FrontendStack, "FrontendStack");

// sst.config.ts
var sst_config_default = {
  config() {
    return {
      name: "charcot",
      region: "us-east-1"
    };
  },
  stacks(app) {
    const stage = app.stage;
    app.stack(CommonStack, {
      id: "common",
      stackName: `${stage}-${app.name}-common`,
      tags: { created_by: "sst" }
    }).stack(BackEndPaidAccountStack, {
      id: "backend-paid-account",
      stackName: `${stage}-${app.name}-backend-paid-account`,
      tags: { created_by: "sst" }
    }).stack(FulfillmentStack, {
      id: "fulfillment",
      stackName: `${stage}-${app.name}-fulfillment`,
      tags: { created_by: "sst" }
    }).stack(FrontendStack, {
      id: "frontend",
      stackName: `${stage}-${app.name}-frontend`,
      tags: { created_by: "sst" }
    }).stack(BackEndOdpStack, {
      id: "backend-odp",
      stackName: `${stage}-${app.name}-backend-odp`,
      tags: { created_by: "sst" }
    });
  }
};
export {
  sst_config_default as default
};
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsic3RhY2tzL0NvbW1vblN0YWNrLnRzIiwgInN0YWNrcy9CYWNrRW5kUGFpZEFjY291bnRTdGFjay50cyIsICJzdGFja3MvQmFja0VuZE9kcFN0YWNrLnRzIiwgInN0YWNrcy9GdWxmaWxsbWVudFN0YWNrLnRzIiwgInN0YWNrcy9Gcm9udEVuZFN0YWNrLnRzIiwgInNzdC5jb25maWcudHMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImltcG9ydCB7IFN1Ym5ldFR5cGUsIFZwYyB9IGZyb20gJ2F3cy1jZGstbGliL2F3cy1lYzInXG5pbXBvcnQgKiBhcyBzc3QgZnJvbSAnc3N0L2NvbnN0cnVjdHMnXG5cbmNvbnN0IGNhbGN1bGF0ZVppcEJ1Y2tldE5hbWUgPSAoc3RhZ2U6IHN0cmluZykgPT4ge1xuICBjb25zdCBidWNrZXRTdWZmaXggPSBzdGFnZSA9PT0gJ3Byb2QnID8gJycgOiBgLSR7c3RhZ2V9YFxuICByZXR1cm4gYGNlcmVicnVtLWltYWdlLXppcCR7YnVja2V0U3VmZml4fWBcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIENvbW1vblN0YWNrKHsgc3RhY2sgfTogc3N0LlN0YWNrQ29udGV4dCkge1xuICBjb25zdCB6aXBCdWNrZXROYW1lID0gY2FsY3VsYXRlWmlwQnVja2V0TmFtZShzdGFjay5zdGFnZSlcblxuICAvKlxuICAgKiBWUEMgaXMgaXJyZWxldmFudCBmb3IgT0RQIGFjY291bnQuIFNlZSBGdWxmaWxsbWVudFN0YWNrIGZvciBpbXBsaWNhdGlvbnMgYW5kIHJlYXNvblxuICAgKiB3ZSBkbyB0aGlzIGZvciB0aGUgT0RQIGFjY291bnQuXG4gICAqL1xuICBpZiAoc3RhY2suYWNjb3VudCA9PT0gJzk1MDg2OTMyNTAwNicpIHtcbiAgICByZXR1cm4geyB6aXBCdWNrZXROYW1lLCB2cGM6IHVuZGVmaW5lZCB9XG4gIH1cbiAgY29uc3QgdnBjID0gbmV3IFZwYyhzdGFjaywgJ0NoYXJjb3RGdWxmaWxsbWVudFNlcnZpY2VWcGMnLCB7XG4gICAgdnBjTmFtZTogYCR7c3RhY2suc3RhZ2V9LWNoYXJjb3RgLFxuICAgIC8vIGlwQWRkcmVzc2VzOiAnJyxcbiAgICBjaWRyOiAnMTAuMS4wLjAvMTcnLFxuICAgIG1heEF6czogMixcbiAgICBzdWJuZXRDb25maWd1cmF0aW9uOiBbXG4gICAgICB7XG4gICAgICAgIG5hbWU6ICdjaGFyY290LWluZ3Jlc3MnLFxuICAgICAgICBzdWJuZXRUeXBlOiBTdWJuZXRUeXBlLlBVQkxJQ1xuICAgICAgfVxuICAgIF1cbiAgfSlcblxuICBzdGFjay5hZGRPdXRwdXRzKHtcbiAgICBWcGNJZDogdnBjLnZwY0lkXG4gIH0pXG5cbiAgcmV0dXJuIHtcbiAgICB2cGNJZDogdnBjLnZwY0lkLFxuICAgIHppcEJ1Y2tldE5hbWUsXG4gICAgdnBjXG4gIH1cbn1cbiIsICJpbXBvcnQgKiBhcyBzc3QgZnJvbSAnc3N0L2NvbnN0cnVjdHMnXG5pbXBvcnQgKiBhcyBpYW0gZnJvbSAnYXdzLWNkay1saWIvYXdzLWlhbSdcbmltcG9ydCB7IEJ1Y2tldCBhcyBTM0J1Y2tldCwgRXZlbnRUeXBlIH0gZnJvbSAnYXdzLWNkay1saWIvYXdzLXMzJ1xuaW1wb3J0ICogYXMgczNOb3RpZmljYXRpb25zIGZyb20gJ2F3cy1jZGstbGliL2F3cy1zMy1ub3RpZmljYXRpb25zJ1xuaW1wb3J0IHsgU3RyaW5nQXR0cmlidXRlIH0gZnJvbSAnYXdzLWNkay1saWIvYXdzLWNvZ25pdG8nXG5pbXBvcnQgeyBDZXJ0aWZpY2F0ZSB9IGZyb20gJ2F3cy1jZGstbGliL2F3cy1jZXJ0aWZpY2F0ZW1hbmFnZXInXG5pbXBvcnQgKiBhcyByb3V0ZTUzIGZyb20gJ2F3cy1jZGstbGliL2F3cy1yb3V0ZTUzJ1xuaW1wb3J0IHsgRHVyYXRpb24gfSBmcm9tICdhd3MtY2RrLWxpYidcbmltcG9ydCB7IENvbnN0cnVjdCB9IGZyb20gJ2NvbnN0cnVjdHMnXG5cbi8qKlxuICogVGhpcyBzdGFjayBkZWZpbmVzIHRoZSBDaGFyY290IGJhY2tlbmQgQVdTIHBhaWQgYWNjb3VudCBvZiBNdCBTaW5haSBwb3J0aW9uIG9mIHRoZSBhcHBcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIEJhY2tFbmRQYWlkQWNjb3VudFN0YWNrKHsgc3RhY2sgfTogc3N0LlN0YWNrQ29udGV4dCkge1xuICBjb25zdCBzdGFnZSA9IHN0YWNrLnN0YWdlXG5cbiAgLy8gQXV0aFxuICBjb25zdCBhdXRoID0gY29nbml0b1VzZXJQb29sKHN0YWNrKVxuXG4gIC8qXG4gICAqIFNRUyBRdWV1ZShzKVxuICAgKi9cbiAgY29uc3QgY2VyZWJydW1JbWFnZU9yZGVyUXVldWUgPSBuZXcgc3N0LlF1ZXVlKHN0YWNrLCAnY2VyZWJydW0taW1hZ2Utb3JkZXItcXVldWUnLCB7XG4gICAgY2RrOiB7XG4gICAgICBxdWV1ZToge1xuICAgICAgICAvLyBHaXZlIG1heGltdW0gb2YgMTIgaG91cnMgdG8gcHJvY2VzcyBhIHJlcXVlc3QsIHNvbWUgb2YgdGhlbSBjYW4gYmUgbGFyZ2UgYW5kIGluIGZhY3RcbiAgICAgICAgLy8gY2FuIHRha2UgbG9uZ2VyIHRoZW4gMTIgaG91cnMuIFNlZSB0aGUgZnVsZmlsbG1lbnQgbW9kdWxlIGZvciBub3RlcyBvbiBob3cgd2VcbiAgICAgICAgLy8gaGFuZGxlIHJlcXVlc3Qgd2hpY2ggdGFrZSBsb25nZXIgdGhlbiBtYXhpbXVtIHRvIHByb2Nlc3MuXG4gICAgICAgIHZpc2liaWxpdHlUaW1lb3V0OiBEdXJhdGlvbi5ob3VycygxMiksXG4gICAgICAgIHJlY2VpdmVNZXNzYWdlV2FpdFRpbWU6IER1cmF0aW9uLnNlY29uZHMoMjApXG4gICAgICB9XG4gICAgfVxuICB9KVxuXG4gIC8qXG4gICAqIER5bmFtb0RCIFRhYmxlc1xuICAgKi9cbiAgY29uc3QgY2VyZWJydW1JbWFnZU1ldGFEYXRhVGFibGUgPSBuZXcgc3N0LlRhYmxlKHN0YWNrLCAnY2VyZWJydW0taW1hZ2UtbWV0YWRhdGEnLCB7XG4gICAgZmllbGRzOiB7XG4gICAgICBmaWxlTmFtZTogJ3N0cmluZycsXG4gICAgICByZWdpb246ICdzdHJpbmcnLFxuICAgICAgc3RhaW46ICdzdHJpbmcnLFxuICAgICAgYWdlOiAnbnVtYmVyJyxcbiAgICAgIHJhY2U6ICdzdHJpbmcnLFxuICAgICAgc2V4OiAnc3RyaW5nJyxcbiAgICAgIGRpYWdub3NpczogJ3N0cmluZycsXG4gICAgICBzdWJqZWN0TnVtYmVyOiAnbnVtYmVyJyxcbiAgICAgIHVwbG9hZERhdGU6ICdzdHJpbmcnLFxuICAgICAgZW5hYmxlZDogJ3N0cmluZydcbiAgICB9LFxuICAgIHByaW1hcnlJbmRleDogeyBwYXJ0aXRpb25LZXk6ICdmaWxlTmFtZScgfSxcbiAgICBnbG9iYWxJbmRleGVzOiB7XG4gICAgICByZWdpb25JbmRleDogeyBwYXJ0aXRpb25LZXk6ICdyZWdpb24nIH0sXG4gICAgICBzdGFpbkluZGV4OiB7IHBhcnRpdGlvbktleTogJ3N0YWluJyB9LFxuICAgICAgYWdlSW5kZXg6IHsgcGFydGl0aW9uS2V5OiAnYWdlJyB9LFxuICAgICAgcmFjZUluZGV4OiB7IHBhcnRpdGlvbktleTogJ3JhY2UnIH0sXG4gICAgICBzZXhJbmRleDogeyBwYXJ0aXRpb25LZXk6ICdzZXgnIH0sXG4gICAgICBkaWFnbm9zaXNJbmRleDogeyBwYXJ0aXRpb25LZXk6ICdkaWFnbm9zaXMnIH0sXG4gICAgICBzdWJqZWN0TnVtYmVySW5kZXg6IHsgcGFydGl0aW9uS2V5OiAnc3ViamVjdE51bWJlcicgfVxuICAgIH1cbiAgfSlcblxuICBjb25zdCBjZXJlYnJ1bUltYWdlT3JkZXJUYWJsZSA9IG5ldyBzc3QuVGFibGUoc3RhY2ssICdjZXJlYnJ1bS1pbWFnZS1vcmRlcicsIHtcbiAgICBmaWVsZHM6IHtcbiAgICAgIG9yZGVySWQ6ICdzdHJpbmcnLFxuICAgICAgZW1haWw6ICdzdHJpbmcnLFxuICAgICAgY3JlYXRlZDogJ251bWJlcicsXG4gICAgICBmaWx0ZXI6ICdzdHJpbmcnLFxuICAgICAgc3RhdHVzOiAnc3RyaW5nJyxcbiAgICAgIGZ1bGZpbGxlZDogJ251bWJlcicsXG4gICAgICByZW1hcms6ICdzdHJpbmcnLFxuICAgICAgc3FzUmVjZWlwdEhhbmRsZTogJ3N0cmluZycsXG4gICAgICBzaXplOiAnbnVtYmVyJyxcbiAgICAgIGZpbGVDb3VudDogJ251bWJlcidcbiAgICB9LFxuICAgIHByaW1hcnlJbmRleDogeyBwYXJ0aXRpb25LZXk6ICdvcmRlcklkJyB9LFxuICAgIC8vIFRPRE86IElzIHRoZSAnY3JlYXRlZCcgdHMgaW5kZXggbmVlZGVkPz8/XG4gICAgZ2xvYmFsSW5kZXhlczoge1xuICAgICAgY3JlYXRlZEluZGV4OiB7IHBhcnRpdGlvbktleTogJ2NyZWF0ZWQnIH1cbiAgICB9XG4gIH0pXG5cbiAgLy8gTXQgU2luYWkgaGFkIG5vIGNvbmNlcHQgb2Ygc3RhZ2VzIHByaW9yIHRvIENoYXJjb3QsIHNvIG5lZWQgdGhlIGJlbG93IGZvciBiYWNrd2FyZCBjb21wYXRpYmlsaXR5XG4gIC8vIHdpdGggdGhlaXIgc3RhZ2UtbGVzcyBTMyBidWNrZXRzIHdoaWNoIHdlcmUgaW4gcGxhY2UgYWxyZWFkeSBiZWZvcmUgQ2hhcmNvdC4gUmVuYW1pbmdcbiAgLy8gdGhvc2UgZXhpc3RpbmcgYnVja2V0cyBpcyBub3QgYW4gb3B0aW9uXG4gIGNvbnN0IGJ1Y2tldFN1ZmZpeCA9IHN0YWdlID09PSAncHJvZCcgPyAnJyA6IGAtJHtzdGFnZX1gXG4gIGNvbnN0IGNlcmVicnVtSW1hZ2VCdWNrZXROYW1lID0gYG5idHItb2RwLXN0YWdpbmcke2J1Y2tldFN1ZmZpeH1gIC8vIHNvdXJjZSBzMyBidWNrZXRcbiAgY29uc3QgY2VyZWJydW1JbWFnZU9kcEJ1Y2tldE5hbWUgPSBgbmJ0ci1wcm9kdWN0aW9uJHtidWNrZXRTdWZmaXh9YCAvLyB0YXJnZXQgczMgYnVja2V0XG5cbiAgLy8gQnVja2V0cyBhbmQgbm90aWZpY2F0aW9uIHRhcmdldCBmdW5jdGlvbnNcbiAgY29uc3QgaGFuZGxlQ2VyZWJydW1JbWFnZVRyYW5zZmVyID0gbmV3IHNzdC5GdW5jdGlvbihzdGFjaywgJ0hhbmRsZUNlcmVicnVtSW1hZ2VUcmFuc2ZlcicsIHtcbiAgICBmdW5jdGlvbk5hbWU6IGBoYW5kbGUtY2VyZWJydW0taW1hZ2UtdHJhbnNmZXItJHtzdGFnZX1gLFxuICAgIGhhbmRsZXI6ICdzcmMvbGFtYmRhL2NlcmVicnVtLWltYWdlLXRyYW5zZmVyLmhhbmRsZScsXG4gICAgbWVtb3J5U2l6ZTogMTI4LFxuICAgIGluaXRpYWxQb2xpY3k6IFtcbiAgICAgIG5ldyBpYW0uUG9saWN5U3RhdGVtZW50KHtcbiAgICAgICAgZWZmZWN0OiBpYW0uRWZmZWN0LkFMTE9XLFxuICAgICAgICBhY3Rpb25zOiBbJ3MzOkdldE9iamVjdCcsICdzMzpEZWxldGVPYmplY3QnXSxcbiAgICAgICAgcmVzb3VyY2VzOiBbYGFybjphd3M6czM6Ojoke2NlcmVicnVtSW1hZ2VCdWNrZXROYW1lfS8qYF1cbiAgICAgIH0pLFxuICAgICAgbmV3IGlhbS5Qb2xpY3lTdGF0ZW1lbnQoe1xuICAgICAgICBlZmZlY3Q6IGlhbS5FZmZlY3QuQUxMT1csXG4gICAgICAgIGFjdGlvbnM6IFsnczM6UHV0T2JqZWN0J10sXG4gICAgICAgIHJlc291cmNlczogW2Bhcm46YXdzOnMzOjo6JHtjZXJlYnJ1bUltYWdlT2RwQnVja2V0TmFtZX0vKmBdXG4gICAgICB9KVxuICAgIF0sXG4gICAgZW52aXJvbm1lbnQ6IHtcbiAgICAgIENFUkVCUlVNX0lNQUdFX09EUF9CVUNLRVRfTkFNRTogY2VyZWJydW1JbWFnZU9kcEJ1Y2tldE5hbWVcbiAgICB9LFxuICAgIHRpbWVvdXQ6IDkwMFxuICB9KVxuXG4gIGlmIChzdGFnZSA9PT0gJ3Byb2QnKSB7XG4gICAgLypcbiAgICAgICAqIEluICdwcm9kJyBzdGFnZSBidWNrZXQgd2FzIGFscmVhZHkgdGhlcmUgYmVmb3JlIGluY2VwdGlvblxuICAgICAgICogb2YgQ2hhcmNvdCwgc28gaGF2ZSB0byB3b3JrIHdpdGggd2hhdCB3YXMgdGhlcmUgYWxyZWFkeSAoSS5lLlxuICAgICAgICogdW5hYmxlIHRvIGRyb3AgYW5kIHJlY3JlYXRlIGl0XG4gICAgICAgKi9cbiAgICBjb25zdCBsb2FkZWRCdWNrZXQgPSBTM0J1Y2tldC5mcm9tQnVja2V0TmFtZShzdGFjaywgJ0J1Y2tldExvYWRlZEJ5TmFtZScsIGNlcmVicnVtSW1hZ2VCdWNrZXROYW1lKVxuICAgIGxvYWRlZEJ1Y2tldC5hZGRFdmVudE5vdGlmaWNhdGlvbihFdmVudFR5cGUuT0JKRUNUX0NSRUFURUQsIG5ldyBzM05vdGlmaWNhdGlvbnMuTGFtYmRhRGVzdGluYXRpb24oaGFuZGxlQ2VyZWJydW1JbWFnZVRyYW5zZmVyKSlcbiAgfSBlbHNlIHtcbiAgICBjb25zdCBjZXJlYnJ1bUltYWdlQnVja2V0ID0gbmV3IHNzdC5CdWNrZXQoc3RhY2ssIGNlcmVicnVtSW1hZ2VCdWNrZXROYW1lLCB7XG4gICAgICBuYW1lOiBjZXJlYnJ1bUltYWdlQnVja2V0TmFtZSxcbiAgICAgIG5vdGlmaWNhdGlvbnM6IHtcbiAgICAgICAgbXlOb3RpZmljYXRpb246IHtcbiAgICAgICAgICB0eXBlOiAnZnVuY3Rpb24nLFxuICAgICAgICAgIGZ1bmN0aW9uOiBoYW5kbGVDZXJlYnJ1bUltYWdlVHJhbnNmZXIsXG4gICAgICAgICAgZXZlbnRzOiBbJ29iamVjdF9jcmVhdGVkJ11cbiAgICAgICAgfVxuICAgICAgfVxuICAgIH0pXG4gICAgY2VyZWJydW1JbWFnZUJ1Y2tldC5hdHRhY2hQZXJtaXNzaW9ucyhbJ3MzJ10pXG4gIH1cblxuICAvLyBDcmVhdGUgYW4gSFRUUCBBUElcbiAgY29uc3QgaG9zdGVkWm9uZSA9IHJvdXRlNTMuSG9zdGVkWm9uZS5mcm9tSG9zdGVkWm9uZUF0dHJpYnV0ZXMoc3RhY2ssICdIb3N0ZWRab25lJywge1xuICAgIGhvc3RlZFpvbmVJZDogJ1owMzQxMTYzMzAzQVNaV01XMVlUUycsXG4gICAgem9uZU5hbWU6ICdtb3VudHNpbmFpY2hhcmNvdC5vcmcnXG4gIH0pXG4gIC8qXG4gICAgICogTm90ZTogVy9vIGV4cGxpY2l0bHkgcGFzc2luZyBpbiBob3N0ZWRab25lLCB3YXMgZ2V0dGluZzpcbiAgICAgKiAgICdJdCBzZWVtcyB5b3UgYXJlIGNvbmZpZ3VyaW5nIGN1c3RvbSBkb21haW5zIGZvciB5b3UgVVJMLiBBbmQgU1NUIGlzIG5vdCBhYmxlIHRvIGZpbmQgdGhlIGhvc3RlZCB6b25lIFwibW91bnRzaW5haWNoYXJjb3Qub3JnXCIgaW4geW91ciBBV1MgUm91dGUgNTMgYWNjb3VudC4gUGxlYXNlIGRvdWJsZSBjaGVjayBhbmQgbWFrZSBzdXJlIHRoZSB6b25lIGV4aXN0cywgb3IgcGFzcyBpbiBhIGRpZmZlcmVudCB6b25lLidcbiAgICAgKi9cbiAgY29uc3QgYXBpID0gbmV3IHNzdC5BcGkoc3RhY2ssICdBcGknLCB7XG4gICAgY3VzdG9tRG9tYWluOiB7XG4gICAgICBkb21haW5OYW1lOiBgJHtzdGFnZSA9PT0gJ3Byb2QnID8gJ2FwaS5tb3VudHNpbmFpY2hhcmNvdC5vcmcnIDogYGFwaS0ke3N0YWdlfS5tb3VudHNpbmFpY2hhcmNvdC5vcmdgfWAsXG4gICAgICBjZGs6IHtcbiAgICAgICAgaG9zdGVkWm9uZSxcbiAgICAgICAgY2VydGlmaWNhdGU6IENlcnRpZmljYXRlLmZyb21DZXJ0aWZpY2F0ZUFybihzdGFjaywgJ015Q2VydCcsICdhcm46YXdzOmFjbTp1cy1lYXN0LTE6MDQ1Mzg3MTQzMTI3OmNlcnRpZmljYXRlLzEwMDRmNTdmLWE1NDQtNDc2ZC04YTMxLTViODc4YTcxYzI3NicpXG4gICAgICB9XG4gICAgfSxcbiAgICByb3V0ZXM6IHtcbiAgICAgICdQT1NUIC9jZXJlYnJ1bS1pbWFnZXMnOiB7XG4gICAgICAgIGZ1bmN0aW9uOiB7XG4gICAgICAgICAgZnVuY3Rpb25OYW1lOiBgY3JlYXRlLWNlcmVicnVtLWltYWdlLW1ldGFkYXRhLSR7c3RhZ2V9YCxcbiAgICAgICAgICBoYW5kbGVyOiAnc3JjL2xhbWJkYS9jZXJlYnJ1bS1pbWFnZS1tZXRhZGF0YS5jcmVhdGUnLFxuICAgICAgICAgIGluaXRpYWxQb2xpY3k6IFtcbiAgICAgICAgICAgIG5ldyBpYW0uUG9saWN5U3RhdGVtZW50KHtcbiAgICAgICAgICAgICAgZWZmZWN0OiBpYW0uRWZmZWN0LkFMTE9XLFxuICAgICAgICAgICAgICBhY3Rpb25zOiBbJ2R5bmFtb2RiOlB1dEl0ZW0nXSxcbiAgICAgICAgICAgICAgcmVzb3VyY2VzOiBbY2VyZWJydW1JbWFnZU1ldGFEYXRhVGFibGUudGFibGVBcm5dXG4gICAgICAgICAgICB9KV0sXG4gICAgICAgICAgZW52aXJvbm1lbnQ6IHtcbiAgICAgICAgICAgIENFUkVCUlVNX0lNQUdFX01FVEFEQVRBX1RBQkxFX05BTUU6IGNlcmVicnVtSW1hZ2VNZXRhRGF0YVRhYmxlLnRhYmxlTmFtZVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfSxcbiAgICAgICdHRVQgL2NlcmVicnVtLWltYWdlcyc6IHtcbiAgICAgICAgZnVuY3Rpb246IHtcbiAgICAgICAgICBmdW5jdGlvbk5hbWU6IGBoYW5kbGUtY2VyZWJydW0taW1hZ2Utc2VhcmNoLSR7c3RhZ2V9YCxcbiAgICAgICAgICBoYW5kbGVyOiAnc3JjL2xhbWJkYS9jZXJlYnJ1bS1pbWFnZS1zZWFyY2guc2VhcmNoJyxcbiAgICAgICAgICBpbml0aWFsUG9saWN5OiBbXG4gICAgICAgICAgICBuZXcgaWFtLlBvbGljeVN0YXRlbWVudCh7XG4gICAgICAgICAgICAgIGVmZmVjdDogaWFtLkVmZmVjdC5BTExPVyxcbiAgICAgICAgICAgICAgYWN0aW9uczogWydkeW5hbW9kYjpRdWVyeScsICdkeW5hbW9kYjpTY2FuJ10sXG4gICAgICAgICAgICAgIHJlc291cmNlczogW2NlcmVicnVtSW1hZ2VNZXRhRGF0YVRhYmxlLnRhYmxlQXJuLCBgJHtjZXJlYnJ1bUltYWdlTWV0YURhdGFUYWJsZS50YWJsZUFybn0vaW5kZXgvKmBdXG4gICAgICAgICAgICB9KV0sXG4gICAgICAgICAgZW52aXJvbm1lbnQ6IHtcbiAgICAgICAgICAgIENFUkVCUlVNX0lNQUdFX01FVEFEQVRBX1RBQkxFX05BTUU6IGNlcmVicnVtSW1hZ2VNZXRhRGF0YVRhYmxlLnRhYmxlTmFtZVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfSxcbiAgICAgICdHRVQgL2NlcmVicnVtLWltYWdlcy97ZGltZW5zaW9ufSc6IHtcbiAgICAgICAgZnVuY3Rpb246IHtcbiAgICAgICAgICBmdW5jdGlvbk5hbWU6IGBoYW5kbGUtY2VyZWJydW0taW1hZ2UtZGltZW5zaW9uLSR7c3RhZ2V9YCxcbiAgICAgICAgICBoYW5kbGVyOiAnc3JjL2xhbWJkYS9jZXJlYnJ1bS1pbWFnZS1zZWFyY2guZGltZW5zaW9uJyxcbiAgICAgICAgICBpbml0aWFsUG9saWN5OiBbXG4gICAgICAgICAgICBuZXcgaWFtLlBvbGljeVN0YXRlbWVudCh7XG4gICAgICAgICAgICAgIGVmZmVjdDogaWFtLkVmZmVjdC5BTExPVyxcbiAgICAgICAgICAgICAgYWN0aW9uczogWydkeW5hbW9kYjpRdWVyeScsICdkeW5hbW9kYjpTY2FuJ10sXG4gICAgICAgICAgICAgIHJlc291cmNlczogW2NlcmVicnVtSW1hZ2VNZXRhRGF0YVRhYmxlLnRhYmxlQXJuLCBgJHtjZXJlYnJ1bUltYWdlTWV0YURhdGFUYWJsZS50YWJsZUFybn0vaW5kZXgvKmBdXG4gICAgICAgICAgICB9KV0sXG4gICAgICAgICAgZW52aXJvbm1lbnQ6IHtcbiAgICAgICAgICAgIENFUkVCUlVNX0lNQUdFX01FVEFEQVRBX1RBQkxFX05BTUU6IGNlcmVicnVtSW1hZ2VNZXRhRGF0YVRhYmxlLnRhYmxlTmFtZVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfSxcbiAgICAgICdHRVQgL2NlcmVicnVtLWltYWdlLXVzZXJzL3tlbWFpbH0nOiB7XG4gICAgICAgIGZ1bmN0aW9uOiB7XG4gICAgICAgICAgZnVuY3Rpb25OYW1lOiBgcmV0cmlldmUtY2VyZWJydW0taW1hZ2UtdXNlci0ke3N0YWdlfWAsXG4gICAgICAgICAgaGFuZGxlcjogJ3NyYy9sYW1iZGEvY2VyZWJydW0taW1hZ2UtdXNlci5yZXRyaWV2ZScsXG4gICAgICAgICAgaW5pdGlhbFBvbGljeTogW1xuICAgICAgICAgICAgbmV3IGlhbS5Qb2xpY3lTdGF0ZW1lbnQoe1xuICAgICAgICAgICAgICBlZmZlY3Q6IGlhbS5FZmZlY3QuQUxMT1csXG4gICAgICAgICAgICAgIGFjdGlvbnM6IFsnY29nbml0by1pZHA6QWRtaW5HZXRVc2VyJ10sXG4gICAgICAgICAgICAgIHJlc291cmNlczogW2F1dGgudXNlclBvb2xBcm5dXG4gICAgICAgICAgICB9KV0sXG4gICAgICAgICAgZW52aXJvbm1lbnQ6IHtcbiAgICAgICAgICAgIENFUkVCUlVNX0NPR05JVE9fVVNFUl9QT09MX0lEOiBhdXRoLnVzZXJQb29sSWRcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH0sXG4gICAgICAnUFVUIC9jZXJlYnJ1bS1pbWFnZS11c2Vycy97ZW1haWx9Jzoge1xuICAgICAgICBmdW5jdGlvbjoge1xuICAgICAgICAgIGZ1bmN0aW9uTmFtZTogYHVwZGF0ZS1jZXJlYnJ1bS1pbWFnZS11c2VyLSR7c3RhZ2V9YCxcbiAgICAgICAgICBoYW5kbGVyOiAnc3JjL2xhbWJkYS9jZXJlYnJ1bS1pbWFnZS11c2VyLnVwZGF0ZScsXG4gICAgICAgICAgaW5pdGlhbFBvbGljeTogW1xuICAgICAgICAgICAgbmV3IGlhbS5Qb2xpY3lTdGF0ZW1lbnQoe1xuICAgICAgICAgICAgICBlZmZlY3Q6IGlhbS5FZmZlY3QuQUxMT1csXG4gICAgICAgICAgICAgIGFjdGlvbnM6IFsnY29nbml0by1pZHA6QWRtaW5VcGRhdGVVc2VyQXR0cmlidXRlcycsICdjb2duaXRvLWlkcDpBZG1pblNldFVzZXJQYXNzd29yZCddLFxuICAgICAgICAgICAgICByZXNvdXJjZXM6IFthdXRoLnVzZXJQb29sQXJuXVxuICAgICAgICAgICAgfSldLFxuICAgICAgICAgIGVudmlyb25tZW50OiB7XG4gICAgICAgICAgICBDRVJFQlJVTV9DT0dOSVRPX1VTRVJfUE9PTF9JRDogYXV0aC51c2VyUG9vbElkXG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9LFxuICAgICAgJ0dFVCAvY2VyZWJydW0taW1hZ2Utb3JkZXJzJzoge1xuICAgICAgICBmdW5jdGlvbjoge1xuICAgICAgICAgIGZ1bmN0aW9uTmFtZTogYHJldHJpZXZlLWNlcmVicnVtLWltYWdlLW9yZGVyLSR7c3RhZ2V9YCxcbiAgICAgICAgICBoYW5kbGVyOiAnc3JjL2xhbWJkYS9jZXJlYnJ1bS1pbWFnZS1vcmRlci5yZXRyaWV2ZScsXG4gICAgICAgICAgaW5pdGlhbFBvbGljeTogW1xuICAgICAgICAgICAgbmV3IGlhbS5Qb2xpY3lTdGF0ZW1lbnQoe1xuICAgICAgICAgICAgICBlZmZlY3Q6IGlhbS5FZmZlY3QuQUxMT1csXG4gICAgICAgICAgICAgIGFjdGlvbnM6IFsnZHluYW1vZGI6U2NhbiddLFxuICAgICAgICAgICAgICByZXNvdXJjZXM6IFtjZXJlYnJ1bUltYWdlT3JkZXJUYWJsZS50YWJsZUFybl1cbiAgICAgICAgICAgIH0pLFxuICAgICAgICAgICAgbmV3IGlhbS5Qb2xpY3lTdGF0ZW1lbnQoe1xuICAgICAgICAgICAgICBlZmZlY3Q6IGlhbS5FZmZlY3QuQUxMT1csXG4gICAgICAgICAgICAgIGFjdGlvbnM6IFsnY29nbml0by1pZHA6QWRtaW5HZXRVc2VyJ10sXG4gICAgICAgICAgICAgIHJlc291cmNlczogW2F1dGgudXNlclBvb2xBcm5dXG4gICAgICAgICAgICB9KV0sXG4gICAgICAgICAgZW52aXJvbm1lbnQ6IHtcbiAgICAgICAgICAgIENFUkVCUlVNX0lNQUdFX09SREVSX1RBQkxFX05BTUU6IGNlcmVicnVtSW1hZ2VPcmRlclRhYmxlLnRhYmxlTmFtZSxcbiAgICAgICAgICAgIENFUkVCUlVNX0NPR05JVE9fVVNFUl9QT09MX0lEOiBhdXRoLnVzZXJQb29sSWRcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH0sXG4gICAgICAnUE9TVCAvY2VyZWJydW0taW1hZ2Utb3JkZXJzJzoge1xuICAgICAgICBhdXRob3JpemVyOiAnaWFtJyxcbiAgICAgICAgZnVuY3Rpb246IHtcbiAgICAgICAgICBmdW5jdGlvbk5hbWU6IGBjcmVhdGUtY2VyZWJydW0taW1hZ2Utb3JkZXItJHtzdGFnZX1gLFxuICAgICAgICAgIGhhbmRsZXI6ICdzcmMvbGFtYmRhL2NlcmVicnVtLWltYWdlLW9yZGVyLmNyZWF0ZScsXG4gICAgICAgICAgaW5pdGlhbFBvbGljeTogW1xuICAgICAgICAgICAgbmV3IGlhbS5Qb2xpY3lTdGF0ZW1lbnQoe1xuICAgICAgICAgICAgICBlZmZlY3Q6IGlhbS5FZmZlY3QuQUxMT1csXG4gICAgICAgICAgICAgIGFjdGlvbnM6IFsnZHluYW1vZGI6UHV0SXRlbSddLFxuICAgICAgICAgICAgICByZXNvdXJjZXM6IFtjZXJlYnJ1bUltYWdlT3JkZXJUYWJsZS50YWJsZUFybl1cbiAgICAgICAgICAgIH0pLFxuICAgICAgICAgICAgbmV3IGlhbS5Qb2xpY3lTdGF0ZW1lbnQoe1xuICAgICAgICAgICAgICBlZmZlY3Q6IGlhbS5FZmZlY3QuQUxMT1csXG4gICAgICAgICAgICAgIGFjdGlvbnM6IFsnZHluYW1vZGI6UXVlcnknLCAnZHluYW1vZGI6U2NhbiddLFxuICAgICAgICAgICAgICByZXNvdXJjZXM6IFtjZXJlYnJ1bUltYWdlTWV0YURhdGFUYWJsZS50YWJsZUFybl1cbiAgICAgICAgICAgIH0pLFxuICAgICAgICAgICAgbmV3IGlhbS5Qb2xpY3lTdGF0ZW1lbnQoe1xuICAgICAgICAgICAgICBlZmZlY3Q6IGlhbS5FZmZlY3QuQUxMT1csXG4gICAgICAgICAgICAgIGFjdGlvbnM6IFsnc3FzOlNlbmRNZXNzYWdlJ10sXG4gICAgICAgICAgICAgIHJlc291cmNlczogW2NlcmVicnVtSW1hZ2VPcmRlclF1ZXVlLnF1ZXVlQXJuXVxuICAgICAgICAgICAgfSldLFxuICAgICAgICAgIGVudmlyb25tZW50OiB7XG4gICAgICAgICAgICBDRVJFQlJVTV9JTUFHRV9PUkRFUl9UQUJMRV9OQU1FOiBjZXJlYnJ1bUltYWdlT3JkZXJUYWJsZS50YWJsZU5hbWUsXG4gICAgICAgICAgICBDRVJFQlJVTV9JTUFHRV9PUkRFUl9RVUVVRV9VUkw6IGNlcmVicnVtSW1hZ2VPcmRlclF1ZXVlLnF1ZXVlVXJsLFxuICAgICAgICAgICAgQ0VSRUJSVU1fSU1BR0VfTUVUQURBVEFfVEFCTEVfTkFNRTogY2VyZWJydW1JbWFnZU1ldGFEYXRhVGFibGUudGFibGVOYW1lXG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9LFxuICAgICAgJ0RFTEVURSAvY2VyZWJydW0taW1hZ2Utb3JkZXJzL3tvcmRlcklkfSc6IHtcbiAgICAgICAgZnVuY3Rpb246IHtcbiAgICAgICAgICBmdW5jdGlvbk5hbWU6IGBjYW5jZWwtY2VyZWJydW0taW1hZ2Utb3JkZXItJHtzdGFnZX1gLFxuICAgICAgICAgIGhhbmRsZXI6ICdzcmMvbGFtYmRhL2NlcmVicnVtLWltYWdlLW9yZGVyLmNhbmNlbCcsXG4gICAgICAgICAgaW5pdGlhbFBvbGljeTogW1xuICAgICAgICAgICAgbmV3IGlhbS5Qb2xpY3lTdGF0ZW1lbnQoe1xuICAgICAgICAgICAgICBlZmZlY3Q6IGlhbS5FZmZlY3QuQUxMT1csXG4gICAgICAgICAgICAgIGFjdGlvbnM6IFsnZHluYW1vZGI6VXBkYXRlSXRlbScsICdkeW5hbW9kYjpHZXRJdGVtJ10sXG4gICAgICAgICAgICAgIHJlc291cmNlczogW2NlcmVicnVtSW1hZ2VPcmRlclRhYmxlLnRhYmxlQXJuXVxuICAgICAgICAgICAgfSksXG4gICAgICAgICAgICBuZXcgaWFtLlBvbGljeVN0YXRlbWVudCh7XG4gICAgICAgICAgICAgIGVmZmVjdDogaWFtLkVmZmVjdC5BTExPVyxcbiAgICAgICAgICAgICAgYWN0aW9uczogWydjb2duaXRvLWlkcDpBZG1pbkdldFVzZXInXSxcbiAgICAgICAgICAgICAgcmVzb3VyY2VzOiBbYXV0aC51c2VyUG9vbEFybl1cbiAgICAgICAgICAgIH0pXG4gICAgICAgICAgXSxcbiAgICAgICAgICBlbnZpcm9ubWVudDoge1xuICAgICAgICAgICAgQ0VSRUJSVU1fSU1BR0VfT1JERVJfVEFCTEVfTkFNRTogY2VyZWJydW1JbWFnZU9yZGVyVGFibGUudGFibGVOYW1lLFxuICAgICAgICAgICAgQ0VSRUJSVU1fQ09HTklUT19VU0VSX1BPT0xfSUQ6IGF1dGgudXNlclBvb2xJZFxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgfSlcblxuICAvLyBUT0RPOiBJcyB0aGlzIG5lZWRlZD8gV2hhdCBoYXBwZW5zIGlmIEkgd2VyZSB0byByZW1vdmU/IFdvdWxkIGxvZ2dlZCBpbiB1c2Vyc1xuICAvLyAgICAgICBiZSBhYmxlIHRvIGhpdCB0aGlzIGVuZHBvaW50LCBidXQgbm90IGFub24gb25lcz8/Pz8/IChoZWFkIHNjcmF0Y2gpXG4gIC8vICAgICAgIEV4cGVyaW1lbnQsXG4gIC8vICAgICAgIFtSRUZ8aHR0cHM6Ly9zc3QuZGV2L2NoYXB0ZXJzL2FkZGluZy1hdXRoLXRvLW91ci1zZXJ2ZXJsZXNzLWFwcC5odG1sfFwiVGhlIGF0dGFjaFBlcm1pc3Npb25zRm9yQXV0aFVzZXJzIGZ1bmN0aW9uIGFsbG93cyB1cyB0byBzcGVjaWZ5IHRoZSByZXNvdXJjZXMgb3VyIGF1dGhlbnRpY2F0ZWQgdXNlcnMgaGF2ZSBhY2Nlc3MgdG8uXCJdXG4gIGF1dGguYXR0YWNoUGVybWlzc2lvbnNGb3JBdXRoVXNlcnMoc3RhY2ssIFthcGldKVxuXG4gIHN0YWNrLmFkZE91dHB1dHMoe1xuICAgIEFwaUVuZHBvaW50OiBhcGkuY3VzdG9tRG9tYWluVXJsIHx8IGFwaS51cmwsXG4gICAgUmVnaW9uOiBzdGFjay5yZWdpb24sXG4gICAgVXNlclBvb2xJZDogYXV0aC51c2VyUG9vbElkLFxuICAgIENvZ25pdG9JZGVudGl0eVBvb2xJZDogYXV0aC5jb2duaXRvSWRlbnRpdHlQb29sSWQhLFxuICAgIFVzZXJQb29sQ2xpZW50SWQ6IGF1dGgudXNlclBvb2xDbGllbnRJZCxcbiAgICBIYW5kbGVDZXJlYnJ1bUltYWdlVHJhbnNmZXJSb2xlQXJuOiBoYW5kbGVDZXJlYnJ1bUltYWdlVHJhbnNmZXI/LnJvbGU/LnJvbGVBcm4gYXMgc3RyaW5nLFxuICAgIENlcmVicnVtSW1hZ2VPcmRlclRhYmxlQXJuOiBjZXJlYnJ1bUltYWdlT3JkZXJUYWJsZS50YWJsZUFybixcbiAgICBDZXJlYnJ1bUltYWdlTWV0YWRhdGFUYWJsZUFybjogY2VyZWJydW1JbWFnZU1ldGFEYXRhVGFibGUudGFibGVBcm4sXG4gICAgQ2VyZWJydW1JbWFnZU9yZGVyUXVldWVBcm46IGNlcmVicnVtSW1hZ2VPcmRlclF1ZXVlLnF1ZXVlQXJuLFxuICAgIENlcmVicnVtSW1hZ2VPcmRlclF1ZXVlVXJsOiBjZXJlYnJ1bUltYWdlT3JkZXJRdWV1ZS5xdWV1ZVVybCxcbiAgICBDZXJlYnJ1bUltYWdlT3JkZXJRdWV1ZU5hbWU6IGNlcmVicnVtSW1hZ2VPcmRlclF1ZXVlLnF1ZXVlTmFtZVxuICB9KVxuXG4gIHJldHVybiB7XG4gICAgYXBpLFxuICAgIGhhbmRsZUNlcmVicnVtSW1hZ2VUcmFuc2ZlclJvbGVBcm46IGhhbmRsZUNlcmVicnVtSW1hZ2VUcmFuc2Zlcj8ucm9sZT8ucm9sZUFybiBhcyBzdHJpbmcsXG4gICAgdXNlclBvb2xJZDogYXV0aC51c2VyUG9vbElkLFxuICAgIHVzZXJQb29sQ2xpZW50SWQ6IGF1dGgudXNlclBvb2xDbGllbnRJZCxcbiAgICBjb2duaXRvSWRlbnRpdHlQb29sSWQ6IGF1dGguY29nbml0b0lkZW50aXR5UG9vbElkLFxuICAgIGNlcmVicnVtSW1hZ2VPcmRlclRhYmxlQXJuOiBjZXJlYnJ1bUltYWdlT3JkZXJUYWJsZS50YWJsZUFybixcbiAgICBjZXJlYnJ1bUltYWdlTWV0YWRhdGFUYWJsZUFybjogY2VyZWJydW1JbWFnZU1ldGFEYXRhVGFibGUudGFibGVBcm4sXG4gICAgY2VyZWJydW1JbWFnZU9yZGVyUXVldWVBcm46IGNlcmVicnVtSW1hZ2VPcmRlclF1ZXVlLnF1ZXVlQXJuXG4gIH1cbn1cblxuY29uc3QgY29nbml0b1VzZXJQb29sID0gKHNjb3BlOiBDb25zdHJ1Y3QpID0+IG5ldyBzc3QuQ29nbml0byhzY29wZSwgJ0F1dGgnLCB7XG4gIGxvZ2luOiBbJ2VtYWlsJ10sXG4gIGNkazoge1xuICAgIHVzZXJQb29sOiB7XG4gICAgICBjdXN0b21BdHRyaWJ1dGVzOiB7XG4gICAgICAgIGRlZ3JlZTogbmV3IFN0cmluZ0F0dHJpYnV0ZSh7XG4gICAgICAgICAgbWluTGVuOiAxLFxuICAgICAgICAgIG1heExlbjogMjU2LFxuICAgICAgICAgIG11dGFibGU6IHRydWVcbiAgICAgICAgfSksXG4gICAgICAgIGluc3RpdHV0aW9uTmFtZTogbmV3IFN0cmluZ0F0dHJpYnV0ZSh7XG4gICAgICAgICAgbWluTGVuOiAxLFxuICAgICAgICAgIG1heExlbjogMjU2LFxuICAgICAgICAgIG11dGFibGU6IHRydWVcbiAgICAgICAgfSksXG4gICAgICAgIGluc3RpdHV0aW9uQWRkcmVzczogbmV3IFN0cmluZ0F0dHJpYnV0ZSh7XG4gICAgICAgICAgbWluTGVuOiAxLFxuICAgICAgICAgIG1heExlbjogMjU2LFxuICAgICAgICAgIG11dGFibGU6IHRydWVcbiAgICAgICAgfSksXG4gICAgICAgIGFyZWFzT2ZJbnRlcmVzdDogbmV3IFN0cmluZ0F0dHJpYnV0ZSh7XG4gICAgICAgICAgbWluTGVuOiAxLFxuICAgICAgICAgIG1heExlbjogMjU2LFxuICAgICAgICAgIG11dGFibGU6IHRydWVcbiAgICAgICAgfSksXG4gICAgICAgIGludGVuZGVkVXNlOiBuZXcgU3RyaW5nQXR0cmlidXRlKHtcbiAgICAgICAgICBtaW5MZW46IDEsXG4gICAgICAgICAgbWF4TGVuOiA1MDAsXG4gICAgICAgICAgbXV0YWJsZTogdHJ1ZVxuICAgICAgICB9KVxuICAgICAgfVxuICAgIH1cbiAgfVxufSlcbiIsICJpbXBvcnQgKiBhcyBzc3QgZnJvbSAnc3N0L2NvbnN0cnVjdHMnXG5pbXBvcnQgKiBhcyBpYW0gZnJvbSAnYXdzLWNkay1saWIvYXdzLWlhbSdcbmltcG9ydCB7IEJ1Y2tldCBhcyBTM0J1Y2tldCB9IGZyb20gJ2F3cy1jZGstbGliL2F3cy1zMydcbmltcG9ydCB7IENvbW1vblN0YWNrIH0gZnJvbSAnLi9Db21tb25TdGFjaydcbmltcG9ydCB7IHVzZSB9IGZyb20gJ3NzdC9jb25zdHJ1Y3RzJ1xuaW1wb3J0IHsgRnVsZmlsbG1lbnRTdGFjayB9IGZyb20gJy4vRnVsZmlsbG1lbnRTdGFjaydcbmltcG9ydCB7IEJhY2tFbmRQYWlkQWNjb3VudFN0YWNrIH0gZnJvbSAnLi9CYWNrRW5kUGFpZEFjY291bnRTdGFjaydcblxuLyoqXG4gKiBUaGlzIHN0YWNrIGRlZmluZXMgdGhlIENoYXJjb3QgYmFja2VuZCBwb3J0aW9uIG9mIHRoZSBBV1MgT0RQIGFjY291bnQgb2YgTXQgU2luYWkuIFRoaXMgc3RhY2tcbiAqIDxzdHJvbmc+ZGVwZW5kczwvc3Ryb25nPiBvbiB0aGUgQmFja0VuZFBhaWRBY2NvdW50U3RhY2sgZm9yIEFXUyBwYWlkIGFjY291bnQgdG8gaGF2ZSBydW4gZmlyc3QsIHRoZXJlZm9yZVxuICogdGhpcyBzaG91bGQgYmUgcnVuIGFmdGVyIHRoZSBBV1MgcGFpZCBhY2NvdW50IEJhY2tFbmRQYWlkQWNjb3VudFN0YWNrIGhhcyBiZWVuIGRlcGxveWVkLiBUaGVcbiAqIHJlYXNvbiBpcyB0aGF0IHRoaXMgc3RhY2sgZXhwZWN0cyBhcyBpbnB1dHMgdGhlIG91dHB1dHMgZnJvbSBCYWNrRW5kUGFpZEFjY291bnRTdGFjaywgZm9yIGV4YW1wbGVcbiAqIEFSTidzIG9mIExhbWJkYSdzIHRoYXQgc2hvdWxkIGJlIGdyYW50ZWQgcGVybWlzc2lvbiB0byB3cml0ZSB0byB0aGUgT0RQIGltYWdlIGJ1Y2tldFxuICogZHVyaW5nIHRoZSBpbWFnZSB0cmFuc2ZlciBwcm9jZXNzLiBUaGUgc2NyaXB0ICdkZXBsb3kubWpzJyBvcmNoZXN0cmF0ZXMgYWxsIG9mIHRoaXMsIHNlZSB0aGF0XG4gKiBmb3IgbW9yZSBkZXRhaWxzLlxuICovXG5leHBvcnQgZnVuY3Rpb24gQmFja0VuZE9kcFN0YWNrKHsgc3RhY2sgfTogc3N0LlN0YWNrQ29udGV4dCkge1xuICBjb25zdCB7IHppcEJ1Y2tldE5hbWUgfSA9IHVzZShDb21tb25TdGFjaylcbiAgbGV0IHsgZnVsZmlsbG1lbnRTZXJ2aWNlVGFza1JvbGVBcm4gfSA9IHVzZShGdWxmaWxsbWVudFN0YWNrKVxuICBsZXQgeyBoYW5kbGVDZXJlYnJ1bUltYWdlVHJhbnNmZXJSb2xlQXJuIH0gPSB1c2UoQmFja0VuZFBhaWRBY2NvdW50U3RhY2spXG5cbiAgZnVsZmlsbG1lbnRTZXJ2aWNlVGFza1JvbGVBcm4gPSBwcm9jZXNzLmVudi5GdWxmaWxsbWVudFNlcnZpY2VUYXNrUm9sZUFybiB8fCBmdWxmaWxsbWVudFNlcnZpY2VUYXNrUm9sZUFyblxuICBoYW5kbGVDZXJlYnJ1bUltYWdlVHJhbnNmZXJSb2xlQXJuID0gcHJvY2Vzcy5lbnYuSGFuZGxlQ2VyZWJydW1JbWFnZVRyYW5zZmVyUm9sZUFybiB8fCBoYW5kbGVDZXJlYnJ1bUltYWdlVHJhbnNmZXJSb2xlQXJuXG5cbiAgY29uc3Qgc3RhZ2UgPSBzdGFjay5zdGFnZVxuICAvLyBTZWUgY29tbWVudCBpbiBCYWNrRW5kUGFpZEFjY291bnRTdGFjay50cyBmb3IgdGhlIHJlYXNvbiBvZiB0aGlzIGxvZ2ljLFxuICAvLyBzYW1lIGFwcGxpZXMgaGVyZVxuICBjb25zdCBidWNrZXRTdWZmaXggPSBzdGFnZSA9PT0gJ3Byb2QnID8gJycgOiBgLSR7c3RhZ2V9YFxuICBjb25zdCBjZXJlYnJ1bUltYWdlT2RwQnVja2V0TmFtZSA9IGBuYnRyLXByb2R1Y3Rpb24ke2J1Y2tldFN1ZmZpeH1gXG5cbiAgY29uc3QgY2VyZWJydW1JbWFnZVppcEJ1Y2tldE5hbWUgPSB6aXBCdWNrZXROYW1lIVxuXG4gIC8vIEJ1Y2tldHNcbiAgbGV0IGNlcmVicnVtSW1hZ2VPZHBCdWNrZXRcbiAgaWYgKHN0YWdlID09PSAncHJvZCcpIHtcbiAgICAvLyBJbiBQUk9EIHRoZSBicmFpbiBpbWFnZSBidWNrZXQgYWxyZWFkeSBleGlzdHMsIHNvIGp1c3QgbG9hZCBpdFxuICAgIGNlcmVicnVtSW1hZ2VPZHBCdWNrZXQgPSBTM0J1Y2tldC5mcm9tQnVja2V0TmFtZShzdGFjaywgJ09EUEJ1Y2tldExvYWRlZEJ5TmFtZScsIGNlcmVicnVtSW1hZ2VPZHBCdWNrZXROYW1lKVxuICB9IGVsc2Uge1xuICAgIGNlcmVicnVtSW1hZ2VPZHBCdWNrZXQgPSBuZXcgc3N0LkJ1Y2tldChzdGFjaywgY2VyZWJydW1JbWFnZU9kcEJ1Y2tldE5hbWUsIHtcbiAgICAgIG5hbWU6IGNlcmVicnVtSW1hZ2VPZHBCdWNrZXROYW1lXG4gICAgfSkuY2RrLmJ1Y2tldFxuICB9XG5cbiAgY2VyZWJydW1JbWFnZU9kcEJ1Y2tldC5hZGRUb1Jlc291cmNlUG9saWN5KFxuICAgIG5ldyBpYW0uUG9saWN5U3RhdGVtZW50KHtcbiAgICAgIGVmZmVjdDogaWFtLkVmZmVjdC5BTExPVyxcbiAgICAgIHByaW5jaXBhbHM6IFtuZXcgaWFtLkFyblByaW5jaXBhbChoYW5kbGVDZXJlYnJ1bUltYWdlVHJhbnNmZXJSb2xlQXJuKV0sXG4gICAgICBhY3Rpb25zOiBbJ3MzOlB1dE9iamVjdCddLFxuICAgICAgcmVzb3VyY2VzOiBbYCR7Y2VyZWJydW1JbWFnZU9kcEJ1Y2tldC5idWNrZXRBcm59LypgXVxuICAgIH0pKVxuXG4gIGNvbnN0IGNlcmVicnVtSW1hZ2VaaXBCdWNrZXQgPSBuZXcgc3N0LkJ1Y2tldChzdGFjaywgY2VyZWJydW1JbWFnZVppcEJ1Y2tldE5hbWUsIHtcbiAgICBuYW1lOiBjZXJlYnJ1bUltYWdlWmlwQnVja2V0TmFtZVxuICB9KVxuXG4gIC8qXG4gICAqIEdyYW50IGZ1bGZpbGxtZW50IHNlcnZpY2UgcGVybXMgdG8gcHV0IFppcCBmaWxlIGluXG4gICAqIGRlc3RpbmF0aW9uIGJ1Y2tldC4gVGhlICdzMzpHZXRPYmplY3QnIGlzIG5lZWRlZCB0byBhbGxvdyBmb3IgdGhlXG4gICAqIHNpZ25lZCBVUkwgZG93bmxvYWRcbiAgICovXG4gIGNlcmVicnVtSW1hZ2VaaXBCdWNrZXQuY2RrLmJ1Y2tldC5hZGRUb1Jlc291cmNlUG9saWN5KFxuICAgIG5ldyBpYW0uUG9saWN5U3RhdGVtZW50KHtcbiAgICAgIGVmZmVjdDogaWFtLkVmZmVjdC5BTExPVyxcbiAgICAgIHByaW5jaXBhbHM6IFtuZXcgaWFtLkFyblByaW5jaXBhbChmdWxmaWxsbWVudFNlcnZpY2VUYXNrUm9sZUFybildLFxuICAgICAgYWN0aW9uczogWydzMzpHZXRPYmplY3QnLCAnczM6UHV0T2JqZWN0J10sXG4gICAgICByZXNvdXJjZXM6IFtgJHtjZXJlYnJ1bUltYWdlWmlwQnVja2V0LmJ1Y2tldEFybn0vKmBdXG4gICAgfSkpXG5cbiAgLypcbiAgICogQWxzbyBncmFudCBmdWxmaWxsbWVudCBwZXJtcyB0byByZWFkIHRoZSBpbWFnZXMgdGhhdCBhcmUgdG8gYmUgemlwcGVkLiBUaGUgTGlzdE9iamVjdCBwb2xpY3kgaXMgbmVlZGVkXG4gICAqIGJlY2F1c2UgdGhlIFppcCBvcGVyYXRpb24gbmVlZHMgdG8gbGlzdCBvdXQgY29udGVudHMgb2YgZm9sZGVycyBpbiBvcmRlciB0byBidWlsZCBmaW5hbFxuICAgKiBsaXN0IG9mIG9iamVjdHMgdG8gWmlwXG4gICAqL1xuICBjZXJlYnJ1bUltYWdlT2RwQnVja2V0LmFkZFRvUmVzb3VyY2VQb2xpY3koXG4gICAgbmV3IGlhbS5Qb2xpY3lTdGF0ZW1lbnQoe1xuICAgICAgc2lkOiAnQWxsb3cgQ2hhcmNvdCBGdWxmaWxsbWVudCBTZXJ2aWNlIHRvIFJlYWQgT2JqZWN0cycsXG4gICAgICBlZmZlY3Q6IGlhbS5FZmZlY3QuQUxMT1csXG4gICAgICBwcmluY2lwYWxzOiBbbmV3IGlhbS5Bcm5QcmluY2lwYWwoZnVsZmlsbG1lbnRTZXJ2aWNlVGFza1JvbGVBcm4pXSxcbiAgICAgIGFjdGlvbnM6IFsnczM6R2V0T2JqZWN0J10sXG4gICAgICByZXNvdXJjZXM6IFtgJHtjZXJlYnJ1bUltYWdlT2RwQnVja2V0LmJ1Y2tldEFybn0vKmBdXG4gICAgfSkpXG5cbiAgY2VyZWJydW1JbWFnZU9kcEJ1Y2tldC5hZGRUb1Jlc291cmNlUG9saWN5KFxuICAgIG5ldyBpYW0uUG9saWN5U3RhdGVtZW50KHtcbiAgICAgIHNpZDogJ0FsbG93IENoYXJjb3QgRnVsZmlsbG1lbnQgU2VydmljZSB0byBMaXN0IE9iamVjdHMnLFxuICAgICAgZWZmZWN0OiBpYW0uRWZmZWN0LkFMTE9XLFxuICAgICAgcHJpbmNpcGFsczogW25ldyBpYW0uQXJuUHJpbmNpcGFsKGZ1bGZpbGxtZW50U2VydmljZVRhc2tSb2xlQXJuKV0sXG4gICAgICBhY3Rpb25zOiBbJ3MzOkxpc3RCdWNrZXQnXSxcbiAgICAgIHJlc291cmNlczogW2Ake2NlcmVicnVtSW1hZ2VPZHBCdWNrZXQuYnVja2V0QXJufWBdXG4gICAgfSkpXG5cbiAgc3RhY2suYWRkT3V0cHV0cyh7XG4gICAgQ2VyZWJydW1JbWFnZU9kcEJ1Y2tldE5hbWU6IGNlcmVicnVtSW1hZ2VPZHBCdWNrZXROYW1lXG4gIH0pXG59XG4iLCAiaW1wb3J0ICogYXMgc3N0IGZyb20gJ3NzdC9jb25zdHJ1Y3RzJ1xuaW1wb3J0IHsgdXNlIH0gZnJvbSAnc3N0L2NvbnN0cnVjdHMnXG5pbXBvcnQgeyBmaWxlVVJMVG9QYXRoIH0gZnJvbSAndXJsJ1xuaW1wb3J0ICogYXMgcm91dGU1MyBmcm9tICdhd3MtY2RrLWxpYi9hd3Mtcm91dGU1MydcbmltcG9ydCAqIGFzIHJvdXRlNTNUYXJnZXRzIGZyb20gJ2F3cy1jZGstbGliL2F3cy1yb3V0ZTUzLXRhcmdldHMnXG5pbXBvcnQgeyBEdXJhdGlvbiB9IGZyb20gJ2F3cy1jZGstbGliJ1xuaW1wb3J0IHsgQ2VydGlmaWNhdGUgfSBmcm9tICdhd3MtY2RrLWxpYi9hd3MtY2VydGlmaWNhdGVtYW5hZ2VyJ1xuaW1wb3J0ICogYXMgaWFtIGZyb20gJ2F3cy1jZGstbGliL2F3cy1pYW0nXG5pbXBvcnQgKiBhcyBzcXMgZnJvbSAnYXdzLWNkay1saWIvYXdzLXNxcydcbmltcG9ydCAqIGFzIGVjcyBmcm9tICdhd3MtY2RrLWxpYi9hd3MtZWNzJ1xuaW1wb3J0IHsgQmFja0VuZFBhaWRBY2NvdW50U3RhY2sgfSBmcm9tICcuL0JhY2tFbmRQYWlkQWNjb3VudFN0YWNrJ1xuaW1wb3J0IHsgQ29tbW9uU3RhY2sgfSBmcm9tICcuL0NvbW1vblN0YWNrJ1xuLy8gZXNsaW50LWRpc2FibGUtbmV4dC1saW5lIGNhbWVsY2FzZVxuaW1wb3J0IGVjc19wYXR0ZXJucyA9IHJlcXVpcmUoJ2F3cy1jZGstbGliL2F3cy1lY3MtcGF0dGVybnMnKVxuaW1wb3J0IHBhdGggPSByZXF1aXJlKCdwYXRoJylcbmltcG9ydCB7IFZwYyB9IGZyb20gJ2F3cy1jZGstbGliL2F3cy1lYzInXG5cbmV4cG9ydCBmdW5jdGlvbiBGdWxmaWxsbWVudFN0YWNrKHsgc3RhY2sgfTogc3N0LlN0YWNrQ29udGV4dCkge1xuICBjb25zdCB7XG4gICAgY2VyZWJydW1JbWFnZU9yZGVyVGFibGVBcm4sXG4gICAgY2VyZWJydW1JbWFnZU9yZGVyUXVldWVBcm4sXG4gICAgY2VyZWJydW1JbWFnZU1ldGFkYXRhVGFibGVBcm5cbiAgfSA9IHVzZShCYWNrRW5kUGFpZEFjY291bnRTdGFjaylcbiAgY29uc3Qge1xuICAgIHppcEJ1Y2tldE5hbWUsXG4gICAgdnBjXG4gIH0gPSB1c2UoQ29tbW9uU3RhY2spXG5cbiAgY29uc3Qgc3RhZ2UgPSBzdGFjay5zdGFnZVxuXG4gIC8qXG4gICAqIFdoZW4gZGVwbG95aW5nIHRoZSBPRFAgYWNjb3VudCBzdGFjayAoQmFja0VuZE9kcFN0YWNrKSwgU1NUL0NESyBzdGlsbCBnb2VzIHRocm91Z2ggYWxsIHRoZSBzdGFja3MgdG8gaW5pdCBhbmQgc3ludGhcbiAgICogdGhlbSwgaW5jbHVkaW5nIHRoaXMgc3RhY2suIFRoaXMgc3RhY2sgdGhyb3dzIGVycm9yIHdoZW4gdHJ5aW5nIHRvIGxvb2t1cCBWUEMgaW4gT0RQIGFjY291bnQgd2hpY2ggd2FzIGNyZWF0ZWQgaW4gcGFpZFxuICAgKiBhY2NvdW50LiBXb3JrYXJvdW5kIGlzIGZvciBDb21tb25TdGFjayB0byBzZXQgdW5kZWZpbmVkICd2cGMnIGZvciBPRFAgYWNjb3VudCwgYW5kIGdpdmUgcHJlZmVyZW5jZSB0byB0aGF0IHVuZGVmaW5lZFxuICAgKiBWUEMgaGVyZSBqdXN0IHRvIGtlZXAgU1NUL0NESyBpbml0L3N5bnRoIHByb2Nlc3MgaGFwcHkuXG4gICAqL1xuICBjb25zdCBjbHVzdGVyID0gbmV3IGVjcy5DbHVzdGVyKHN0YWNrLCAnQ2hhcmNvdEZ1bGZpbGxtZW50U2VydmljZUNsdXN0ZXInLCB7XG4gICAgY2x1c3Rlck5hbWU6IGAke3N0YWdlfS1jaGFyY290YCxcbiAgICB2cGM6ICF2cGMgPyB2cGMgOiAocHJvY2Vzcy5lbnYuVnBjSWQgPyBWcGMuZnJvbUxvb2t1cChzdGFjaywgJ1ZQQycsIHsgdnBjSWQ6IHByb2Nlc3MuZW52LlZwY0lkIH0pIDogdnBjKVxuICB9KVxuXG4gIGNvbnN0IHRhc2tEZWZpbml0aW9uID0gbmV3IGVjcy5GYXJnYXRlVGFza0RlZmluaXRpb24oc3RhY2ssICdDaGFyY290RnVsZmlsbG1lbnRTZXJ2aWNlVGFza0RlZmluaXRpb24nLCB7XG4gICAgcnVudGltZVBsYXRmb3JtOiB7XG4gICAgICBvcGVyYXRpbmdTeXN0ZW1GYW1pbHk6IGVjcy5PcGVyYXRpbmdTeXN0ZW1GYW1pbHkuTElOVVgsXG4gICAgICBjcHVBcmNoaXRlY3R1cmU6IGVjcy5DcHVBcmNoaXRlY3R1cmUuQVJNNjRcbiAgICB9LFxuICAgIGVwaGVtZXJhbFN0b3JhZ2VHaUI6IDIwMCxcbiAgICBjcHU6IDIwNDgsXG4gICAgbWVtb3J5TGltaXRNaUI6IDE2Mzg0XG4gIH0pXG5cbiAgY29uc3QgY29udGFpbmVyRGVmaW5pdGlvbiA9IG5ldyBlY3MuQ29udGFpbmVyRGVmaW5pdGlvbihzdGFjaywgJ0NoYXJjb3RGdWxmaWxsbWVudFNlcnZpY2VDb250YWluZXJEZWZpbml0aW9uJywge1xuICAgIC8vIGVzbGludC1kaXNhYmxlLW5leHQtbGluZSBAdHlwZXNjcmlwdC1lc2xpbnQvYmFuLXRzLWNvbW1lbnRcbiAgICAvLyBAdHMtaWdub3JlXG4gICAgaW1hZ2U6IGVjcy5Db250YWluZXJJbWFnZS5mcm9tQXNzZXQocGF0aC5yZXNvbHZlKHBhdGguZGlybmFtZShmaWxlVVJMVG9QYXRoKGltcG9ydC5tZXRhLnVybCkpLCAnZnVsZmlsbG1lbnQnKSwge1xuICAgICAgYnVpbGRBcmdzOiB7XG4gICAgICAgIFNUQUdFOiBzdGFnZVxuICAgICAgfVxuICAgIH0pLFxuICAgIHRhc2tEZWZpbml0aW9uLFxuICAgIGxvZ2dpbmc6IG5ldyBlY3MuQXdzTG9nRHJpdmVyKHtcbiAgICAgIHN0cmVhbVByZWZpeDogYCR7c3RhZ2V9LWNoYXJjb3QtZnVsZmlsbG1lbnRgXG4gICAgfSlcbiAgfSlcblxuICBjb250YWluZXJEZWZpbml0aW9uLmFkZFBvcnRNYXBwaW5ncyh7XG4gICAgY29udGFpbmVyUG9ydDogODBcbiAgfSlcblxuICAvKlxuICAgKiBJbnN0YW50aWF0ZSBGYXJnYXRlIFNlcnZpY2Ugd2l0aCBhIGNsdXN0ZXIgYW5kIGEgbG9jYWwgaW1hZ2UgdGhhdCBnZXRzXG4gICAqIHVwbG9hZGVkIHRvIGFuIFMzIHN0YWdpbmcgYnVja2V0IHByaW9yIHRvIGJlaW5nIHVwbG9hZGVkIHRvIEVDUi5cbiAgICogQSBuZXcgcmVwb3NpdG9yeSBpcyBjcmVhdGVkIGluIEVDUiBhbmQgdGhlIEZhcmdhdGUgc2VydmljZSBpcyBjcmVhdGVkXG4gICAqIHdpdGggdGhlIGltYWdlIGZyb20gRUNSLlxuICAgKi9cbiAgY29uc3Qgc2VydmljZSA9IG5ldyBlY3NfcGF0dGVybnMuQXBwbGljYXRpb25Mb2FkQmFsYW5jZWRGYXJnYXRlU2VydmljZShzdGFjaywgJ0NoYXJjb3RGdWxmaWxsbWVudFNlcnZpY2UnLCB7XG4gICAgdGFza0RlZmluaXRpb24sXG4gICAgc2VydmljZU5hbWU6IGAke3N0YWdlfS1jaGFyY290LWZ1bGZpbGxtZW50YCxcbiAgICBhc3NpZ25QdWJsaWNJcDogdHJ1ZSwgLy8gVE9ETzogSGlkZSBpdCBmcm9tIHRoZSB3b3JsZD9cbiAgICBjZXJ0aWZpY2F0ZTogQ2VydGlmaWNhdGUuZnJvbUNlcnRpZmljYXRlQXJuKHN0YWNrLCAnTXlDZXJ0JywgJ2Fybjphd3M6YWNtOnVzLWVhc3QtMTowNDUzODcxNDMxMjc6Y2VydGlmaWNhdGUvMTAwNGY1N2YtYTU0NC00NzZkLThhMzEtNWI4NzhhNzFjMjc2JyksXG4gICAgZGVzaXJlZENvdW50OiAxLFxuICAgIGNsdXN0ZXJcbiAgfSlcblxuICBjb25zdCBzY2FsYWJsZVRhc2tDb3VudCA9IHNlcnZpY2Uuc2VydmljZS5hdXRvU2NhbGVUYXNrQ291bnQoe1xuICAgIG1heENhcGFjaXR5OiA1LFxuICAgIG1pbkNhcGFjaXR5OiAxXG4gIH0pXG5cbiAgY29uc3Qgb3JkZXJRdWV1ZSA9IHNxcy5RdWV1ZS5mcm9tUXVldWVBcm4oc3RhY2ssICdvcmRlclF1ZXVlJywgY2VyZWJydW1JbWFnZU9yZGVyUXVldWVBcm4pXG4gIC8qXG4gICAqIEtlZXAgNSBpbnN0YW5jZXMgcnVubmluZyBhcyBsb25nIGFzIHRoZXJlIGFyZSBpbi1mbGlnaHQgcmVxdWVzdHNcbiAgICovXG4gIHNjYWxhYmxlVGFza0NvdW50LnNjYWxlT25NZXRyaWMoJ2Z1bGZpbGxtZW50U2NhbGVPdXRQb2xpY3knLCB7XG4gICAgbWV0cmljOiBvcmRlclF1ZXVlLm1ldHJpY0FwcHJveGltYXRlTnVtYmVyT2ZNZXNzYWdlc05vdFZpc2libGUoKSxcbiAgICAvLyBhZGp1c3RtZW50VHlwZTogQWRqdXN0bWVudFR5cGUuRVhBQ1RfQ0FQQUNJVFksXG4gICAgc2NhbGluZ1N0ZXBzOiBbXG4gICAgICB7XG4gICAgICAgIGxvd2VyOiAxLFxuICAgICAgICBjaGFuZ2U6ICs0XG4gICAgICB9LFxuICAgICAge1xuICAgICAgICB1cHBlcjogMCxcbiAgICAgICAgY2hhbmdlOiAtNFxuICAgICAgfVxuICAgIF1cbiAgfSlcblxuICAvLyBBZGQgcG9saWN5IHN0YXRlbWVudHMgc28gdGhhdCBFQ1MgdGFza3MgY2FuIHBlcmZvcm0vY2Fycnkgb3V0IHRoZSBwZXJ0aW5lbnQgYWN0aW9uc1xuICBjb25zdCBjZXJlYnJ1bUltYWdlT2RwQnVja2V0TmFtZVByb2RTdGFnZSA9ICduYnRyLXByb2R1Y3Rpb24nXG4gIHNlcnZpY2UudGFza0RlZmluaXRpb24udGFza1JvbGUuYWRkVG9QcmluY2lwYWxQb2xpY3kobmV3IGlhbS5Qb2xpY3lTdGF0ZW1lbnQoe1xuICAgIGVmZmVjdDogaWFtLkVmZmVjdC5BTExPVyxcbiAgICBhY3Rpb25zOiBbJ2R5bmFtb2RiOkdldEl0ZW0nLCAnZHluYW1vZGI6VXBkYXRlSXRlbSddLFxuICAgIHJlc291cmNlczogW2NlcmVicnVtSW1hZ2VPcmRlclRhYmxlQXJuXVxuICB9KSlcbiAgc2VydmljZS50YXNrRGVmaW5pdGlvbi50YXNrUm9sZS5hZGRUb1ByaW5jaXBhbFBvbGljeShuZXcgaWFtLlBvbGljeVN0YXRlbWVudCh7XG4gICAgZWZmZWN0OiBpYW0uRWZmZWN0LkFMTE9XLFxuICAgIGFjdGlvbnM6IFsnc3FzOlJlY2VpdmVNZXNzYWdlJywgJ3NxczpEZWxldGVNZXNzYWdlJ10sXG4gICAgcmVzb3VyY2VzOiBbY2VyZWJydW1JbWFnZU9yZGVyUXVldWVBcm4gYXMgc3RyaW5nXVxuICB9KSlcbiAgc2VydmljZS50YXNrRGVmaW5pdGlvbi50YXNrUm9sZS5hZGRUb1ByaW5jaXBhbFBvbGljeShuZXcgaWFtLlBvbGljeVN0YXRlbWVudCh7XG4gICAgZWZmZWN0OiBpYW0uRWZmZWN0LkFMTE9XLFxuICAgIGFjdGlvbnM6IFsnZHluYW1vZGI6R2V0SXRlbSddLFxuICAgIHJlc291cmNlczogW2NlcmVicnVtSW1hZ2VNZXRhZGF0YVRhYmxlQXJuXVxuICB9KSlcbiAgc2VydmljZS50YXNrRGVmaW5pdGlvbi50YXNrUm9sZS5hZGRUb1ByaW5jaXBhbFBvbGljeShuZXcgaWFtLlBvbGljeVN0YXRlbWVudCh7XG4gICAgZWZmZWN0OiBpYW0uRWZmZWN0LkFMTE9XLFxuICAgIGFjdGlvbnM6IFsnczM6R2V0T2JqZWN0J10sXG4gICAgcmVzb3VyY2VzOiBbYGFybjphd3M6czM6Ojoke2NlcmVicnVtSW1hZ2VPZHBCdWNrZXROYW1lUHJvZFN0YWdlfS8qYF1cbiAgfSkpXG4gIHNlcnZpY2UudGFza0RlZmluaXRpb24udGFza1JvbGUuYWRkVG9QcmluY2lwYWxQb2xpY3kobmV3IGlhbS5Qb2xpY3lTdGF0ZW1lbnQoe1xuICAgIGVmZmVjdDogaWFtLkVmZmVjdC5BTExPVyxcbiAgICBhY3Rpb25zOiBbJ3MzOkxpc3RCdWNrZXQnXSxcbiAgICByZXNvdXJjZXM6IFtgYXJuOmF3czpzMzo6OiR7Y2VyZWJydW1JbWFnZU9kcEJ1Y2tldE5hbWVQcm9kU3RhZ2V9YF1cbiAgfSkpXG4gIHNlcnZpY2UudGFza0RlZmluaXRpb24udGFza1JvbGUuYWRkVG9QcmluY2lwYWxQb2xpY3kobmV3IGlhbS5Qb2xpY3lTdGF0ZW1lbnQoe1xuICAgIGVmZmVjdDogaWFtLkVmZmVjdC5BTExPVyxcbiAgICBhY3Rpb25zOiBbJ3MzOlB1dE9iamVjdCcsICdzMzpHZXRPYmplY3QnXSxcbiAgICByZXNvdXJjZXM6IFtgYXJuOmF3czpzMzo6OiR7emlwQnVja2V0TmFtZX0vKmBdXG4gIH0pKVxuICBzZXJ2aWNlLnRhc2tEZWZpbml0aW9uLnRhc2tSb2xlLmFkZFRvUHJpbmNpcGFsUG9saWN5KG5ldyBpYW0uUG9saWN5U3RhdGVtZW50KHtcbiAgICBlZmZlY3Q6IGlhbS5FZmZlY3QuQUxMT1csXG4gICAgYWN0aW9uczogWydzZXM6U2VuZEVtYWlsJ10sXG4gICAgcmVzb3VyY2VzOiBbJyonXVxuICB9KSlcblxuICBzZXJ2aWNlLnRhcmdldEdyb3VwLmNvbmZpZ3VyZUhlYWx0aENoZWNrKHtcbiAgICBwYXRoOiAnL2FjdHVhdG9yL2hlYWx0aCdcbiAgfSlcblxuICAvLyBhc3NvY2lhdGUgdGhlIEFMQiBETlMgbmFtZSB3aXRoIGEgZml4ZWQgZG9tYWluXG4gIGNvbnN0IGhvc3RlZFpvbmUgPSByb3V0ZTUzLkhvc3RlZFpvbmUuZnJvbUhvc3RlZFpvbmVBdHRyaWJ1dGVzKHN0YWNrLCAnSG9zdGVkWm9uZScsIHtcbiAgICBob3N0ZWRab25lSWQ6ICdaMDM0MTE2MzMwM0FTWldNVzFZVFMnLFxuICAgIHpvbmVOYW1lOiAnbW91bnRzaW5haWNoYXJjb3Qub3JnJ1xuICB9KVxuXG4gIC8vIGVzbGludC1kaXNhYmxlLW5leHQtbGluZSBuby1uZXdcbiAgbmV3IHJvdXRlNTMuQVJlY29yZChzdGFjaywgJ2NoYXJjb3QtZnVsZmlsbG1lbnQtZG5zLWEtcmVjb3JkJywge1xuICAgIHJlY29yZE5hbWU6IHN0YWdlID09PSAncHJvZCcgPyAnZnVsZmlsbG1lbnQnIDogYGZ1bGZpbGxtZW50LSR7c3RhZ2V9YCxcbiAgICB6b25lOiBob3N0ZWRab25lLFxuICAgIHRhcmdldDogcm91dGU1My5SZWNvcmRUYXJnZXQuZnJvbUFsaWFzKG5ldyByb3V0ZTUzVGFyZ2V0cy5Mb2FkQmFsYW5jZXJUYXJnZXQoc2VydmljZS5sb2FkQmFsYW5jZXIpKSxcbiAgICB0dGw6IER1cmF0aW9uLm1pbnV0ZXMoMSlcbiAgfSlcblxuICBzdGFjay5hZGRPdXRwdXRzKHtcbiAgICBGdWxmaWxsbWVudFNlcnZpY2VUYXNrUm9sZUFybjogc2VydmljZS50YXNrRGVmaW5pdGlvbi50YXNrUm9sZS5yb2xlQXJuXG4gIH0pXG4gIHJldHVybiB7IGZ1bGZpbGxtZW50U2VydmljZVRhc2tSb2xlQXJuOiBzZXJ2aWNlLnRhc2tEZWZpbml0aW9uLnRhc2tSb2xlLnJvbGVBcm4gfVxufVxuIiwgImltcG9ydCAqIGFzIHNzdCBmcm9tICdzc3QvY29uc3RydWN0cydcbmltcG9ydCB7IENlcnRpZmljYXRlIH0gZnJvbSAnYXdzLWNkay1saWIvYXdzLWNlcnRpZmljYXRlbWFuYWdlcidcbmltcG9ydCAqIGFzIHJvdXRlNTMgZnJvbSAnYXdzLWNkay1saWIvYXdzLXJvdXRlNTMnXG5pbXBvcnQgeyBCYWNrRW5kUGFpZEFjY291bnRTdGFjayB9IGZyb20gJy4vQmFja0VuZFBhaWRBY2NvdW50U3RhY2snXG5pbXBvcnQgeyB1c2UgfSBmcm9tICdzc3QvY29uc3RydWN0cydcblxuZXhwb3J0IGZ1bmN0aW9uIEZyb250ZW5kU3RhY2soeyBzdGFjayB9OiBzc3QuU3RhY2tDb250ZXh0KSB7XG4gIGNvbnN0IHtcbiAgICBhcGksXG4gICAgdXNlclBvb2xJZCxcbiAgICB1c2VyUG9vbENsaWVudElkLFxuICAgIGNvZ25pdG9JZGVudGl0eVBvb2xJZFxuICB9ID0gdXNlKEJhY2tFbmRQYWlkQWNjb3VudFN0YWNrKVxuXG4gIC8vIERlZmluZSBvdXIgUmVhY3QgYXBwXG4gIGNvbnN0IGVudmlyb25tZW50ID0ge1xuICAgIFJFQUNUX0FQUF9BUElfVVJMOiBwcm9jZXNzLmVudi5BcGlFbmRwb2ludCB8fCBhcGkuY3VzdG9tRG9tYWluVXJsIHx8IGFwaS51cmwsXG4gICAgUkVBQ1RfQVBQX1JFR0lPTjogc3RhY2sucmVnaW9uLFxuICAgIFJFQUNUX0FQUF9VU0VSX1BPT0xfSUQ6IHVzZXJQb29sSWQhLFxuICAgIFJFQUNUX0FQUF9VU0VSX1BPT0xfQ0xJRU5UX0lEOiB1c2VyUG9vbENsaWVudElkISxcbiAgICBSRUFDVF9BUFBfSURFTlRJVFlfUE9PTF9JRDogY29nbml0b0lkZW50aXR5UG9vbElkIVxuICB9XG4gIGNvbnN0IHN0YWdlID0gc3RhY2suc3RhZ2VcbiAgY29uc3QgaG9zdGVkWm9uZSA9IHJvdXRlNTMuSG9zdGVkWm9uZS5mcm9tSG9zdGVkWm9uZUF0dHJpYnV0ZXMoc3RhY2ssICdIb3N0ZWRab25lJywge1xuICAgIGhvc3RlZFpvbmVJZDogJ1owMzQxMTYzMzAzQVNaV01XMVlUUycsXG4gICAgem9uZU5hbWU6ICdtb3VudHNpbmFpY2hhcmNvdC5vcmcnXG4gIH0pXG4gIGNvbnN0IHNpdGUgPSBuZXcgc3N0LlN0YXRpY1NpdGUoc3RhY2ssICdSZWFjdFNpdGUnLCB7XG4gICAgcGF0aDogJ2Zyb250ZW5kJyxcbiAgICBidWlsZENvbW1hbmQ6ICducG0gcnVuIGJ1aWxkJyxcbiAgICBidWlsZE91dHB1dDogJ2J1aWxkJyxcbiAgICBlbnZpcm9ubWVudCxcbiAgICBjdXN0b21Eb21haW46IHtcbiAgICAgIGRvbWFpbk5hbWU6IHN0YWdlID09PSAncHJvZCcgPyAnd3d3Lm1vdW50c2luYWljaGFyY290Lm9yZycgOiBgJHtzdGFnZX0ubW91bnRzaW5haWNoYXJjb3Qub3JnYCxcbiAgICAgIGRvbWFpbkFsaWFzOiBzdGFnZSA9PT0gJ3Byb2QnID8gJ21vdW50c2luYWljaGFyY290Lm9yZycgOiB1bmRlZmluZWQsXG4gICAgICBjZGs6IHtcbiAgICAgICAgaG9zdGVkWm9uZSxcbiAgICAgICAgY2VydGlmaWNhdGU6IENlcnRpZmljYXRlLmZyb21DZXJ0aWZpY2F0ZUFybihzdGFjaywgJ015Q2VydCcsICdhcm46YXdzOmFjbTp1cy1lYXN0LTE6MDQ1Mzg3MTQzMTI3OmNlcnRpZmljYXRlLzEwMDRmNTdmLWE1NDQtNDc2ZC04YTMxLTViODc4YTcxYzI3NicpXG4gICAgICB9XG4gICAgfVxuICB9KVxuXG4gIC8qXG4gICAqIEFmdGVyIFNTVCAyLnggdXBncmFkZSwgbmVlZCB0byBjb21tZW50IG91dCBiZWxvdyBibG9jayBlbnRpcmVseSB3aGVuIHJlbW92aW5nIHN0YWNrOlxuICAgKiBodHRwczovL2Rpc2NvcmQuY29tL2NoYW5uZWxzLzk4Mzg2NTY3MzY1NjcwNTAyNS85ODM4NjY0MTY4MzI4NjQzNTAvMTA3NjU5NzYwMzIyMDg0ODY5MFxuICAgKi9cbiAgaWYgKHN0YWNrLnN0YWdlICE9PSAnZGVidWcnKSB7XG4gICAgc3RhY2suYWRkT3V0cHV0cyh7XG4gICAgICBTaXRlVXJsOiAoc2l0ZS5jdXN0b21Eb21haW5VcmwgfHwgc2l0ZS51cmwpIGFzIHN0cmluZyxcbiAgICAgIERpc3RyaWJ1dGlvbkRvbWFpbjogc2l0ZS5jZGsuZGlzdHJpYnV0aW9uLmRpc3RyaWJ1dGlvbkRvbWFpbk5hbWUsXG4gICAgICBEaXN0cmlidXRpb25JZDogc2l0ZS5jZGsuZGlzdHJpYnV0aW9uLmRpc3RyaWJ1dGlvbklkLFxuICAgICAgRW52aXJvbm1lbnQ6IEpTT04uc3RyaW5naWZ5KGVudmlyb25tZW50LCBudWxsLCAyKVxuICAgIH0pXG4gIH0gZWxzZSB7XG4gICAgc3RhY2suYWRkT3V0cHV0cyh7XG4gICAgICBTaXRlVXJsOiAnZm9vJ1xuICAgIH0pXG4gIH1cbn1cbiIsICJpbXBvcnQgeyBTU1RDb25maWcgfSBmcm9tICdzc3QnXG5pbXBvcnQgeyBDb21tb25TdGFjayB9IGZyb20gJy4vc3RhY2tzL0NvbW1vblN0YWNrJ1xuaW1wb3J0IHsgQmFja0VuZFBhaWRBY2NvdW50U3RhY2sgfSBmcm9tICcuL3N0YWNrcy9CYWNrRW5kUGFpZEFjY291bnRTdGFjaydcbmltcG9ydCB7IEJhY2tFbmRPZHBTdGFjayB9IGZyb20gJy4vc3RhY2tzL0JhY2tFbmRPZHBTdGFjaydcbmltcG9ydCB7IEZyb250ZW5kU3RhY2sgfSBmcm9tICcuL3N0YWNrcy9Gcm9udEVuZFN0YWNrJ1xuaW1wb3J0IHsgRnVsZmlsbG1lbnRTdGFjayB9IGZyb20gJy4vc3RhY2tzL0Z1bGZpbGxtZW50U3RhY2snXG5cbi8vIGVzbGludC1kaXNhYmxlLW5leHQtbGluZSBAdHlwZXNjcmlwdC1lc2xpbnQvYmFuLXRzLWNvbW1lbnRcbi8vIEB0cy1pZ25vcmVcbmV4cG9ydCBkZWZhdWx0IHtcbiAgY29uZmlnKCkge1xuICAgIHJldHVybiB7XG4gICAgICBuYW1lOiAnY2hhcmNvdCcsXG4gICAgICByZWdpb246ICd1cy1lYXN0LTEnXG4gICAgfVxuICB9LFxuICBzdGFja3MoYXBwKSB7XG4gICAgY29uc3Qgc3RhZ2UgPSBhcHAuc3RhZ2VcbiAgICBhcHBcbiAgICAgIC5zdGFjayhDb21tb25TdGFjaywge1xuICAgICAgICBpZDogJ2NvbW1vbicsXG4gICAgICAgIHN0YWNrTmFtZTogYCR7c3RhZ2V9LSR7YXBwLm5hbWV9LWNvbW1vbmAsXG4gICAgICAgIHRhZ3M6IHsgY3JlYXRlZF9ieTogJ3NzdCcgfVxuICAgICAgfSlcbiAgICAgIC5zdGFjayhCYWNrRW5kUGFpZEFjY291bnRTdGFjaywge1xuICAgICAgICBpZDogJ2JhY2tlbmQtcGFpZC1hY2NvdW50JyxcbiAgICAgICAgc3RhY2tOYW1lOiBgJHtzdGFnZX0tJHthcHAubmFtZX0tYmFja2VuZC1wYWlkLWFjY291bnRgLFxuICAgICAgICB0YWdzOiB7IGNyZWF0ZWRfYnk6ICdzc3QnIH1cbiAgICAgIH0pXG4gICAgICAuc3RhY2soRnVsZmlsbG1lbnRTdGFjaywge1xuICAgICAgICBpZDogJ2Z1bGZpbGxtZW50JyxcbiAgICAgICAgc3RhY2tOYW1lOiBgJHtzdGFnZX0tJHthcHAubmFtZX0tZnVsZmlsbG1lbnRgLFxuICAgICAgICB0YWdzOiB7IGNyZWF0ZWRfYnk6ICdzc3QnIH1cbiAgICAgIH0pXG4gICAgICAuc3RhY2soRnJvbnRlbmRTdGFjaywge1xuICAgICAgICBpZDogJ2Zyb250ZW5kJyxcbiAgICAgICAgc3RhY2tOYW1lOiBgJHtzdGFnZX0tJHthcHAubmFtZX0tZnJvbnRlbmRgLFxuICAgICAgICB0YWdzOiB7IGNyZWF0ZWRfYnk6ICdzc3QnIH1cbiAgICAgIH0pXG4gICAgICAuc3RhY2soQmFja0VuZE9kcFN0YWNrLCB7XG4gICAgICAgIGlkOiAnYmFja2VuZC1vZHAnLFxuICAgICAgICBzdGFja05hbWU6IGAke3N0YWdlfS0ke2FwcC5uYW1lfS1iYWNrZW5kLW9kcGAsXG4gICAgICAgIHRhZ3M6IHsgY3JlYXRlZF9ieTogJ3NzdCcgfVxuICAgICAgfSlcbiAgfVxufSBzYXRpc2ZpZXMgU1NUQ29uZmlnXG4iXSwKICAibWFwcGluZ3MiOiAiOzs7Ozs7Ozs7Ozs7QUFBQSxTQUFTLFlBQVksV0FBVztBQUdoQyxJQUFNLHlCQUF5Qix3QkFBQyxVQUFrQjtBQUNoRCxRQUFNLGVBQWUsVUFBVSxTQUFTLEtBQUssSUFBSTtBQUNq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LEVBQ2IsU0FBUztBQUNQLFdBQU87QUFBQSxNQUNMLE1BQU07QUFBQSxNQUNOLFFBQVE7QUFBQSxJQUNWO0FBQUEsRUFDRjtBQUFBLEVBQ0EsT0FBTyxLQUFLO0FBQ1YsVUFBTSxRQUFRLElBQUk7QUFDbEIsUUFDRyxNQUFNLGFBQWE7QUFBQSxNQUNsQixJQUFJO0FBQUEsTUFDSixXQUFXLEdBQUcsU0FBUyxJQUFJO0FBQUEsTUFDM0IsTUFBTSxFQUFFLFlBQVksTUFBTTtBQUFBLElBQzVCLENBQUMsRUFDQSxNQUFNLHlCQUF5QjtBQUFBLE1BQzlCLElBQUk7QUFBQSxNQUNKLFdBQVcsR0FBRyxTQUFTLElBQUk7QUFBQSxNQUMzQixNQUFNLEVBQUUsWUFBWSxNQUFNO0FBQUEsSUFDNUIsQ0FBQyxFQUNBLE1BQU0sa0JBQWtCO0FBQUEsTUFDdkIsSUFBSTtBQUFBLE1BQ0osV0FBVyxHQUFHLFNBQVMsSUFBSTtBQUFBLE1BQzNCLE1BQU0sRUFBRSxZQUFZLE1BQU07QUFBQSxJQUM1QixDQUFDLEVBQ0EsTUFBTSxlQUFlO0FBQUEsTUFDcEIsSUFBSTtBQUFBLE1BQ0osV0FBVyxHQUFHLFNBQVMsSUFBSTtBQUFBLE1BQzNCLE1BQU0sRUFBRSxZQUFZLE1BQU07QUFBQSxJQUM1QixDQUFDLEVBQ0EsTUFBTSxpQkFBaUI7QUFBQSxNQUN0QixJQUFJO0FBQUEsTUFDSixXQUFXLEdBQUcsU0FBUyxJQUFJO0FBQUEsTUFDM0IsTUFBTSxFQUFFLFlBQVksTUFBTTtBQUFBLElBQzVCLENBQUM7QUFBQSxFQUNMO0FBQ0Y7IiwKICAibmFtZXMiOiBbInNzdCIsICJpYW0iLCAiUzNCdWNrZXQiLCAidXNlIiwgInJvdXRlNTMiLCAiRHVyYXRpb24iLCAiQ2VydGlmaWNhdGUiLCAiaWFtIiwgIlZwYyIsICJWcGMiLCAiQ2VydGlmaWNhdGUiLCAiRHVyYXRpb24iLCAidXNlIiwgIlMzQnVja2V0IiwgInNzdCIsICJDZXJ0aWZpY2F0ZSIsICJyb3V0ZTUzIiwgInVzZSIsICJ1c2UiLCAiQ2VydGlmaWNhdGUiXQp9Cg==
