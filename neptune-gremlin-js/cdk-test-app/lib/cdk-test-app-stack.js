const { Stack } = require("aws-cdk-lib")
const cdk = require("aws-cdk-lib")
const ec2 = require("aws-cdk-lib/aws-ec2")
const lambda = require("aws-cdk-lib/aws-lambda")
const neptune = require("@aws-cdk/aws-neptune-alpha")

class CdkTestAppStack extends Stack {
    /**
     *
     * @param {Construct} scope
     * @param {string} id
     * @param {StackProps=} props
     */
    constructor(scope, id, props) {
        super(scope, id, props)

        // Create a dedicated VPC for the cluster
        const vpc = new ec2.Vpc(this, "vpc")

        // Cluster parameter group
        const clusterParameterGroup = new neptune.ClusterParameterGroup(this,
            "ClusterParams",
            {
                description: "Cluster parameter group",
                parameters: {
                    neptune_enable_audit_log: "1",
                },
            },
        )

        // Db parameter group
        const parameterGroup = new neptune.ParameterGroup(this, "DbParams", {
            description: "Db parameter group",
            parameters: {
                neptune_query_timeout: "10000",
            },
        })

        // Create the security group for the cluster
        const clusterSecurityGroup = new ec2.SecurityGroup(this, "ClusterSG", {
            vpc: vpc,
            description: "Neptune Gremlin Test Security Group",
        })

        // Create the cluster
        const cluster = new neptune.DatabaseCluster(this, "cluster", {
            vpc: vpc,
            instanceType: neptune.InstanceType.T3_MEDIUM,
            clusterParameterGroup,
            parameterGroup,
            backupRetention: cdk.Duration.days(7),
            deletionProtection: true,
            securityGroups: [clusterSecurityGroup],
        })

        // Output the writer endpoint host:port
        new cdk.CfnOutput(this, "WriteEndpointOutput", {
            value: cluster.clusterEndpoint.socketAddress,
        })

        // Create a security group for the lambda function
        const lambdaSecurityGroup = new ec2.SecurityGroup(this, "LambdaSG", {
            vpc: vpc,
            description: "Neptune Gremlin Test Lambda Security Group",
        })

        // Add an ingress rule to the cluster's security group from the lambda sg
        const port = cluster.clusterEndpoint.port
        clusterSecurityGroup.addIngressRule(
            lambdaSecurityGroup, ec2.Port.tcp(port))

        // Environment variables for the lambda function
        const envVars = {
            "NEPTUNE_ENDPOINT": cluster.clusterEndpoint.hostname,
            "NEPTUNE_PORT": cluster.clusterEndpoint.port,
            "USE_IAM": "true",
        }

        // Create the integration test Lambda
        const testLambda = new lambda.Function(this, "neptune-gremlin-test", {
            runtime: lambda.Runtime.NODEJS_14_X,
            code: lambda.Code.fromAsset("lambda"),
            handler: "integration-test.handler",
            vpc: vpc,
            timeout: cdk.Duration.seconds(10),
            memorySize: 1536,
            environment: envVars,
            securityGroups: [lambdaSecurityGroup],
        })

        // Give the lambda function access to the cluster
        cluster.grantConnect(testLambda)
    }

}


module.exports = { CdkTestAppStack }
