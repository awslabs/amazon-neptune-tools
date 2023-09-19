# Dynamic Custom Endpoints

Dynamic Custom Endpoints allows you to create [Amazon Neptune custom endpoints](https://docs.aws.amazon.com/neptune/latest/userguide/feature-overview-endpoints.html#feature-custom-endpoint-membership) for a Neptune cluster based on custom specifications with application-meaningful criteria. The endpoints are refreshed every minute by an AWS Lambda function based on these specifications.

Dynamic Custom Endpoints is installed on a per-cluster basis. You can specify multiple endpoints for the same cluster with each installation.

Here's an example of a dynamic custom endpoint specification:

```
{
  "customEndpoints": [
    {
      "customEndpoint": "analytics",
      "specification": {
        "role": {
          "eq": "reader"
        },
        "endpointIsAvailable": {
          "eq": true
        },
        "tags": {
          "any": [
            {
              "Key": "Group",
              "Value": "analytics"
            }
          ]
        }
      }
    }
  ]
}
```

This specification creates a custom endpoint with the endpoint id `analytics`. This endpoint will include any reader instances in the cluster whose endpoints are [considered available](#instance-endpoint-availability), and that have a `Group` tag with the value `analytics`. As you add and remove readers tagged `analytics`, the custom endpoint membership set will change.

Here's a more complex specification, illustrating the use of `and` and `or` operators:

```
{
  "customEndpoints": [
    {
      "customEndpoint": "analytics",
      "specification": {
        "and": [
          {
            "or": [
              {
                "role": "writer",
                "tags": {
                  "all": [
                    {
                      "Key": "Application",
                      "Value": "NeptuneCloudformation"
                    },
                    {
                      "Key": "Name",
                      "Value": "Neptune-test"
                    }
                  ]
                }
              },
              {
                "role": "reader",
                "tags": {
                  "all": [
                    {
                      "Key": "Application",
                      "Value": "NeptuneCloudformation"
                    },
                    {
                      "Key": "Name",
                      "Value": "Neptune-demo"
                    }
                  ]
                }
              }
            ]
          },
          {
            "endpointIsAvailable": true
          }
        ]
      }
    }
  ]
}
```

## Installation

1.  Clone the amazon-neptune-tools repository:

    ```
    git clone https://github.com/awslabs/amazon-neptune-tools.git
    ```

2.  Build the Lambda function (the build script uses Python 3.9):

    ```
    cd amazon-neptune-tools/dynamic-custom-endpoints/lambda
    sh build.sh
    ```

    This creates a zip file: `amazon-neptune-tools/dynamic-custom-endpoints/lambda/target/dynamic_custom_endpoints.zip`. Copy this zip file to an Amazon S3 bucket.
    
3. Provision the Lambda function using the dynamic-custom-endpoints-lambda.json CloudFormation template in the cloudformation-templates directory. The CloudFormation template expects the following input parameters:

   - __NeptuneClusterId__ – Id of your Neptune cluster.
   - __Config__ – JSON document containing your [custom endpoint specification(s)](#custom-endpoint-specifications).
   - __RefreshInterval__ – The number of minutes (1-60) between each refresh of your custom endpoints.
   - __S3Bucket__ – Name of an Amazon S3 bucket in which you have put the Lambda function zip file from Step 2.
   - __S3Key__ – S3 key of the Lambda function zip file from Step 2.
   - __ResourcePrefix__ – (Optional, but recommended) Restricts the Lambda function's access to Neptune database instances and endpoints. By default, the IAM policy for the Lambda function allows the function to describe and list the tags attached to _all_ the Neptune database instances in your account (in the region in which the function is installed), and to modify all custom endpoints in your account (again, in the region in which the function is installed). If you supply a resource prefix here, the IAM policy will be restricted to accessing database instances and custom endpoints whose names begin with this prefix. For example, if you supply `my-cluster-` as the resource prefix, the Lambda will only be able to access database instances and custom endpoints whose names begin `my-cluster`.

4. The solution is installed on a per-cluster basis. You can have only one installation per cluster (but you can specify multiple custom endpoints for a cluster with a single installation). The CloudFormation strack will fail if there is already an installation for a cluster.

5. The Lambda starts managing custom endpoints automatically after the CloudFormation stack completes. If you delete the CloudFormation stack, any custom endpoints created by this solution will _not_ be deleted. You will have to delete these endpoints manually.
   
## Custom Endpoint Specifications

Custom endpoint selection criteria are specified using a JSON document. You supply this JSON as a CloudFormation parameter when you install the solution. The JSON is then stored in a Lambda environment variable, `config`. If you update the contents of this environment variable, the changes will be picked up the next time the Lambda function runs. In this way you can add new custom endpoints, and modify existing custom endpoint specifications.

The specification document has the following format:

```
{
  "customEndpoints": [
    {<endpoint_specification>},
    {<another_endpoint_specification>},
    {<a_third_endpoint_specification>}
  ]
}
```

Each custom endpoint specification takes the following form:

```
{
  "customEndpoint": "<endpoint_id>",
  "specification": {
    ...
  }
}
```

The value you supply for `<endpoint_id>` will form the first part of the endpoint's name. If you have chosen to restrict access to endpoints using a resource prefix (see the [installation](#installation) instructions), then the name here must begin with the resource prefix you used when installing the CloudFormation stack.

Specifications include one or more instance attributes, and the match criteria for these attributes. For example, `role`, is an attribute of all instances in a Neptune cluster; for each instance it has the value of either `writer` or `reader`. To create a custom endpoint that refers to all readers in the cluster, you use the following specification:

 ```
 {
   "customEndpoint": "all-readers",
   "specification": {
     "role": "reader"
   }
 }
```

Specifications typically compose attributes to create more complex match criteria. Instances can be in several states, but clients ought only attempt to connect to those whose endpoints are [considered available](#instance-endpoint-availability). Therefore, we can update our specification to refer to only available reader instances:

 ```
 {
   "customEndpoint": "all-readers",
   "specification": {
     "role": "reader",
     "endpointIsAvailable": true
   }
 }
```

### Empty endpoints

You can create empty custom endpoints. Connections to an empty custom endpoint from a client will fail with an `UnknownHostException` (or similar, depending on the client).

### Propagating endpoint changes

Changes to a cluster's topology will not be seen immediately by your application. The time it takes for endpoint changes to propagate to clients depends on the Lambda's refresh interval (one minute by default), the time it takes for Neptune to apply changes to an endpoint's membership lists (typically several seconds, but sometimes 30 seconds or more), and the frequency with which your application's Neptune clients refresh the connections in their connection pools.

## Instance attributes

You can select Neptune instances based on the following attributes:

  - `instanceId` - __string__
  - `instanceType` - __string__
  - `role` - __string__
  - `status` - __string__
  - `endpointIsAvailable` - __boolean__
  - `endpoint` - __string__
  - `availabilityZone` - __string__
  - `promotionTier` - __number__
  - `tags` - array of `{"Key": "<key>", "Value": "<value>"}` objects
    
### Instance endpoint availability

The list of instance attributes includes a custom helper attribute, `endpointIsAvailable`, which indicates whether an instance's endpoint is considered available. An instance endpoint is considered _likely_ to be available if the `status` of the instance is one of the following: `available`, `backing-up`, `modifying`, `upgrading`.

The full list of database instance states can be found [here](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/accessing-monitoring.html). Note that not all of these states are relevant to Amazon Neptune (for example, `converting-to-vpc` does not apply to Amazon Neptune database instances).

Even if an instance is in one of the states listed above, there is no guarantee that the endpoint is _actually_ available. For example, while an instance is `upgrading`, there can be short periods when the endpoint is _not_ available. During the upgrade process, instances are sometimes restarted, and while this is happening the instance endpoint will not be available, even though the state is upgrading.

### Tags

One of the most useful instance attributes is `tags`. 

Neptune allows you to attach [Amazon Neptune tags](https://docs.aws.amazon.com/neptune/latest/userguide/tagging.html) in the form of name-value pairs to instances in your cluster. Using tags, you can attach application- or domain-meaningful information to your instances, and then use this information to select instances for inclusion in a custom endpoint. 

For example, you could add __Application__ tags (`Key=Application`) to your all your instances, with those dedicated to your sales use cases taking the value __Sales__ (`Value=Sales`). You could then further divide the workload using a __Group__ tag (`Key=Group`) with the values __Reporting__ (`Value=Reporting`) and __Billing__ (`Value=Billing`). With these tags, you can then create a custom endpoint for reporting clients:

 ```
 {
   "customEndpoint": "all-readers",
   "specification": {
     "role": "reader",
     "status": "available",
     "tags": [
       { "Key": "Application", "Value": "Sales" },
       { "Key": "Group", "Value": "Reporting" }
     ]
   }
 }
```

## Operators

You can use a number of different operators to create more complex sepcifications:

  - `and`
  - `or`
  - `equals` (and `eq`)
  - `notEquals` (and `notEq`)
  - `startsWith`
  - `greaterThan` (and `gt`)
  - `lessThan` (and `lt`)
  - `any`
  - `all`
  - `none`

### Operator examples

#### `and`

```
{
  "and": [
    {"role": "writer"},
    {"status": "available"}
  ]
}
```

#### `or`

```
{
  "or": [
    {"status": "available"},
    {"availabilityZone": "eu-west-2"}
  ]
}
```

#### `equals`

```
{"role": {"equals": "reader"} }
```

`equals` is the implicit operator for all instance attributes apart from `tags`. The expression above can be expressed more simply using this implicit operator:

```
{"role": "reader" }
```

#### `notEquals`

```
{"role": {"notEquals": "reader"} }
```

#### `startsWith`

```
{"instanceId": {"startsWith": "neptune-db-"} }
```

#### `greaterThan`

```
{"promotionTier": {"greaterThan": 1} }
```

#### `lessThan`

```
{"promotionTier": {"lessThan": 2} }
```

#### `any`

Match instances whose endpoint is likely available (i.e. instance whose `status` is one of the following states: `available`, `backing-up`, `modifying`, `upgrading`):

```
{"status": {"any": ["available", "backing-up", "modifying", "upgrading"]} }
```

Match instances with any of the following tags:

```
{
  "tags": {
    "any": [
      {
        "Key": "Application",
        "Value": "AnotherApp"
      },
      {
        "Key": "Name",
        "Value": "Neptune-test"
      }
    ]
  }
}
```

You can also use `equals`, `notEquals`, and `startsWith` operators to match a tag __Value__ (but not its __Key__):

```
{
  "tags": {
    "any": [
      {
        "Key": "Application",
        "Value": { "notEquals": "AnotherApp" }
      },
      {
        "Key": "Name",
        "Value": { "startsWith": "Neptune-" }
      }
    ]
  }
}
```

#### `all`

Match instances that have all of the following tags:

```
{
  "tags": {
    "all": [
      {
        "Key": "Application",
        "Value": "AnotherApp"
      },
      {
        "Key": "Name",
        "Value": "Neptune-test"
      }
    ]
  }
}
```

`all` is the implicit operator for arrays of `tags`. The expression above can be expressed more simply using this implicit operator:

```
{
  "tags": [
    {
      "Key": "Application",
      "Value": "AnotherApp"
    },
    {
      "Key": "Name",
      "Value": "Neptune-test"
    }
  ]  
}
```

You can also use `equals`, `notEquals`, and `startsWith` operators to match a tag __Value__ (but not its __Key__):

```
{
  "tags": {
    "all": [
      {
        "Key": "Application",
        "Value": { "notEquals": "AnotherApp" }
      },
      {
        "Key": "Name",
        "Value": { "startsWith": "Neptune-" }
      }
    ]
  }
}
```

#### `none`

Match instances with none of the following tags:

```
{
  "tags": {
    "none": [
      {
        "Key": "Application",
        "Value": "AnotherApp"
      },
      {
        "Key": "Name",
        "Value": "Neptune-test"
      }
    ]
  }
}
```

You can also use `equals`, `notEquals`, and `startsWith` operators to match a tag __Value__ (but not its __Key__):

```
{
  "tags": {
    "none": [
      {
        "Key": "Application",
        "Value": { "notEquals": "AnotherApp" }
      },
      {
        "Key": "Name",
        "Value": { "startsWith": "Neptune-" }
      }
    ]
  }
}
```

## Troubleshooting

Check the CloudWatch logs for the Lambda function.

The successful creation of a new endpoint looks like this (timestamps and UUIDs removed for clarity):

```
[INFO] cluster_id: ianrob-target
[INFO] resource_prefix: ianrob-
[INFO] config: {'customEndpoints': [{'customEndpoint': 'ianrob-all-instances', 'specification': {'endpointIsAvailable': True, 'instanceType': 'db.serverless'}}]}
[INFO] cluster_metadata: {'clusterEndpoint': 'ianrob-target.cluster-abcdefghijkl.us-east-1.neptune.amazonaws.com', 'readerEndpoint': 'ianrob-target.cluster-ro-abcdefghijkl.us-east-1.neptune.amazonaws.com', 'instances': [{'instanceId': 'ianrob-target-instance-0', 'role': 'writer', 'endpoint': 'ianrob-target-instance-0.abcdefghijkl.us-east-1.neptune.amazonaws.com', 'status': 'available', 'endpointIsAvailable': True, 'availabilityZone': 'us-east-1c', 'instanceType': 'db.serverless', 'promotionTier': 1, 'tags': [{'Key': 'Application', 'Value': 'MyApp'}]}, {'instanceId': 'ianrob-target-instance-1', 'role': 'reader', 'endpoint': 'ianrob-target-instance-1.abcdefghijkl.us-east-1.neptune.amazonaws.com', 'status': 'available', 'endpointIsAvailable': True, 'availabilityZone': 'us-east-1b', 'instanceType': 'db.serverless', 'promotionTier': 1, 'tags': [{'Key': 'Application', 'Value': 'MyApp'}]}]}
[INFO] Starting applying config...
[INFO] Starting applying config for endpoint 'ianrob-all-instances'
[INFO] specification: {'endpointIsAvailable': True, 'instanceType': 'db.serverless'}
[INFO] conditions: [field 'endpointIsAvailable' == 'True'] AND [field 'instanceType' == 'db.serverless']
[INFO] instance_ids: ['ianrob-target-instance-0', 'ianrob-target-instance-1']
[INFO] excluded_ids: []
[INFO] Creating custom endpoint...
[INFO] endpoint_id: ianrob-all-instances
[INFO] member_instance_ids: ['ianrob-target-instance-0', 'ianrob-target-instance-1']
[INFO] excluded_ids: []
[INFO] Endpoint 'ianrob-all-instances' created
[INFO] Finished applying config for endpoint 'ianrob-all-instances'
[INFO] Finished applying config
```