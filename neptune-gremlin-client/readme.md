# Deprecation Notice

The Gremlin Client for Amazon Neptune has been migrated to a [new standalone repository](https://github.com/aws/neptune-gremlin-client). Ongoing development and releases will take place in the new repository, and this module here will no longer be maintained.

Any Neptune Gremlin Client related issues should be reported in the Issues section under the new repository.

Version 1.1.0 of the client is the last release of the client from this repository. The new repository is accompanied by a release of version 2.0.0 of the Neptune Gremlin Client.

See [Migrating from version 1 of the Neptune Gremlin Client](https://github.com/aws/neptune-gremlin-client#migrating-from-version-1-of-the-neptune-gremlin-client) if you are migrating an application from version 1.x.x of the Neptune Gremlin Client to version 2.x.x.

# Gremlin Client for Amazon Neptune

A Java Gremlin client for Amazon Neptune that allows you to change the endpoints used by the client as it is running. Includes an agent that can query the Amazon Neptune API for cluster details, and update the client on a periodic basis. You can supply your own custom endpoint selectors to configure the client for a subset of instances in your cluster based on tags, instance types, instance IDs, AZs, etc.

The client also provides support for IAM database authentication, and for connecting to Neptune via a network or application load balancer.

If your application uses a lot of concurrent clients, you should proxy endpoint refresh requests through a Lambda function that periodically queries the Management API and then caches the results on behalf of your clients. This repository includes an AWS Lambda function that can act as a Neptune endpoints information proxy.

## Dependencies

### Maven

```
<dependency>
    <groupId>software.amazon.neptune</groupId>
    <artifactId>gremlin-client</artifactId>
    <version>1.1.0</version>
</dependency>
```

## Documentation

  - [Recent features](#recent-features)
  - [Using the topology aware cluster and client](#using-the-topology-aware-cluster-and-client)
  - [Refreshing endpoints using the ClusterEndpointsRefreshAgent](#refreshing-endpoints-using-the-clusterendpointsrefreshagent)
    - [Providing custom endpoint selection logic using EndpointsSelector](#providing-custom-endpoint-selection-logic-using-endpointsselector)
    - [Connect the ClusterEndpointsRefreshAgent to a Lambda Proxy when you have many clients](#connect-the-clusterendpointsrefreshAgent-to-a-lambda-proxy-when-you-have-many-clients)
       - [Lambda proxy environment variables](#lambda-proxy-environment-variables)
       - [Marking specific endpoints as unavailable](#marking-specific-endpoints-as-unavailable)
       - [Installing the neptune-endpoints-info AWS Lambda function](#installing-the-neptune-endpoints-info-aws-lambda-function)
  - [GremlinClusterBuilder and NeptuneGremlinClusterBuilder](#gremlinclusterbuilder-and-neptunegremlinclusterbuilder)
    - [Credentials](#credentials)
    - [Service region](#service-region)
    - [Connection timeouts and refreshing endpoint addresses after connection failures](#connection-timeouts-and-refreshing-endpoint-addresses-after-connection-failures)
    - [Transactions](#transactions)
  - [Demos](#demos)
    - [RollingSubsetOfEndpointsDemo](#rollingsubsetofendpointsdemo)
    - [RefreshAgentDemo](#refreshagentdemo)
    - [TxDemo](#txdemo)

## Recent features

  - **[New May 2023 – version 1.1.0]** Upgraded to version 3.6.2 of the Java Gremlin driver.
  
  - **[New February 2023]** AWS Lamba proxy now allows you to mark specific endpoints as being unavailable.

  - **[New December 2022 – version 1.0.8]** Now supports transactions against a writer or cluster endpoint.
	
  - **[New November 2022 – version 1.0.8]** The `ClusterEndpointsRefreshAgent.lambdaProxy()` factory method, which creates a `ClusterEndpointsRefreshAgent` that queries an endpoints AWS Lambda proxy, now accepts a custom `EndpointsSelector`, allowing you to use custom endpoint selection strategies with the Lambda proxy.

  - **[New May 24 2022 – version 1.0.6]** Upgraded to version 3.5.2 of the Java Gremlin driver.
  
  - **[New May 24 2022 – version 1.0.5]** Upgraded to version 3.4.13 of the Java Gremlin driver.

  - **[WARNING]** Avoid use of versions, 1.0.3, 1.0.4, and 1.11. 

  - **[New January 2022 – version 1.0.2]** `GremlinClient.chooseConnection()` now respects the connection pool's `maxWaitForConnection` timeout value. Improves the way in which consecutive failures to acquire a connection can trigger endpoint refresh behaviours.

  - **[New July 2021]** Upgraded to version 3.4.12 of the Java Gremlin driver.
  
  - **[New July 2021]** `ClusterEndpointsRefreshAgent` and `GetEndpointsFromLambdaProxy` allow you to supply a region, and a credentials profile name or `AwsCredentialsProvider` object.
  
  - **[New July 2021]** `NeptuneGremlinClusterBuilder` allows you to supply a Neptune service region, and a credentials profile name or `AwsCredentialsProvider` object. The `NeptuneGremlinClusterBuilder` also allows you to supply your own `HandshakeInterceptor` implementation to override the default load balancer-aware SigV4 request signing process.

The Amazon CloudWatch screenshot below shows requests being distributed over 5 instances in a Neptune cluster. The cluster endpoints are being rotated in the client every minute, with 3 endpoints active at any one time. Partway through the run, we deliberately triggered a failover in the cluster.

![Rotating Endpoints](rotating-endpoints.png)

## Using the topology aware cluster and client
 
You create a `GremlinCluster` and `GremlinClient` much as you would a normal cluster and client:

```
GremlinCluster cluster = GremlinClusterBuilder.build()
        .enableSsl(true)
        .addContactPoints("replica-endpoint-1", "replica-endpoint-2", "replica-endpoint-3")
        .port(8182)
        .create();       
 
GremlinClient client = cluster.connect();
 
DriverRemoteConnection connection = DriverRemoteConnection.using(client);
GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(connection);
 
// Use g throughout the lifetime of your application to submit queries to Neptune
 
client.close();
cluster.close();
```
 
Use the `GraphTraversalSource` created here throughout the lifetime of your application, and across threads – just as you would with a regular client. The `GremlinClient` ensures that requests are distributed across the current set of endpoints in a round-robin fashion.
 
The `GremlinClient` has a `refreshEndpoints()` method that allows you to submit a fresh list of endpoint addresses. When the list of endpoints changes, new requests will be distributed across the new set of endpoints.
 
Once you have a reference to a `GremlinClient`, you can call this `refreshEndpoints()` method whenever you discover the cluster topology has changed. You could subscribe to SNS events, for example, and refresh the list whenever an instance is added or removed, or when you detect a failover. To update the list of endpoint addresses:
 
```
client.refreshEndpoints("new-replica-endpoint-1", "new-replica-endpoint-2", "new-replica-endpoint-3")
```
 
Because the cluster topology can change at any moment as a result of both planned and unplanned events, you should wrap all queries with an exception handler. Should a query fail because the underlying client connection has been closed, you can attempt a retry.

## Refreshing endpoints using the ClusterEndpointsRefreshAgent

The `ClusterEndpointsRefreshAgent` allows you to schedule endpoint updates to a client based on a Neptune cluster ID.  The identity under which you're running the agent must be authorized to perform `rds:DescribeDBClusters`,  `rds:DescribeDBInstances` and `rds:ListTagsForResource` for your Neptune cluster.

The following shows how to refresh a client with a cluster's available replica endpoints every 60 seconds:

```
EndpointsType selector = EndpointsType.ReadReplicas;
            
ClusterEndpointsRefreshAgent refreshAgent = new ClusterEndpointsRefreshAgent(
        clusterId,
        selector);

GremlinCluster cluster = GremlinClusterBuilder.build()
        .enableSsl(true)
        .addContactPoints(refreshAgent.getAddresses().get(selector))
        .port(8182)
        .create();

GremlinClient client = cluster.connect();

refreshAgent.startPollingNeptuneAPI(
        (OnNewAddresses) addresses -> client.refreshEndpoints(addresses.get(selector)),
        60,
        TimeUnit.SECONDS);
```

### Providing custom endpoint selection logic using EndpointsSelector

The `ClusterEndpointsRefreshAgent` constructor accepts an `EndpointsSelector` that allows you to add custom endpoint selection logic. The following example shows how to select endpoints for all **Available** instances with a **workload** tag whose value is **analytics**:

```
EndpointsSelector selector = (clusterEndpoint, readerEndpoint, instances) ->
    instances.stream()
        .filter(NeptuneInstanceMetadata::isReader)
        .filter(i -> i.hasTag("workload", "analytics"))
        .filter(NeptuneInstanceMetadata::isAvailable)
        .map(NeptuneInstanceMetadata::getEndpoint)
        .collect(Collectors.toList());

ClusterEndpointsRefreshAgent refreshAgent = new ClusterEndpointsRefreshAgent(
    clusterId,
    selector);

GremlinCluster cluster = GremlinClusterBuilder.build()
    .enableSsl(true)
    .addContactPoints(refreshAgent.getAddresses().get(selector))
    .port(8182)
    .create();

GremlinClient client = cluster.connect();

refreshAgent.startPollingNeptuneAPI(
    (OnNewAddresses) addresses -> client.refreshEndpoints(addresses.get(selector)),
    60,
    TimeUnit.SECONDS);
```

The `EndpointsType` enum provides implementations of `EndpointsSelector` for some common use cases:

  * `EndpointsType.All` –  Returns all available instance (writer and read replicas) endpoints, or, if there are no available instance endpoints, the [reader endpoint](https://docs.aws.amazon.com/neptune/latest/userguide/feature-overview-endpoints.html#feature-overview-reader-endpoints).
  * `EndpointsType.Primary` – Returns the primary (writer) instance endpoint if it is available, or the [cluster endpoint](https://docs.aws.amazon.com/neptune/latest/userguide/feature-overview-endpoints.html#feature-overview-cluster-endpoints) if the primary instance endpoint is not available.
  * `EndpointsType.ReadReplicas` – Returns all available read replica instance endpoints, or, if there are no replica instance endpoints, the [reader endpoint](https://docs.aws.amazon.com/neptune/latest/userguide/feature-overview-endpoints.html#feature-overview-reader-endpoints).
  * `EndpointsType.ClusterEndpoint` – Returns the [cluster endpoint](https://docs.aws.amazon.com/neptune/latest/userguide/feature-overview-endpoints.html#feature-overview-cluster-endpoints).
  * `EndpointsType.ReaderEndpoint` – Returns the [reader endpoint](https://docs.aws.amazon.com/neptune/latest/userguide/feature-overview-endpoints.html#feature-overview-reader-endpoints).


### Connect the ClusterEndpointsRefreshAgent to a Lambda Proxy when you have many clients

When the Neptune Management API experiences a high rate of requests, it starts throttling API calls. If you have a lot of clients frequently polling for endpoint information, your application can very quickly experience throttling (in the form of HTTP 400 throttling exceptions).

Because of this throttling behaviour, if your application uses a lot of concurrent `GremlinClient` and `ClusterEndpointsRefreshAgent` instances, instead of querying the Management API directly, you should proxy endpoint refresh requests through an AWS Lambda function. The Lambda function can periodically query the Management API and then cache the results on behalf of its clients.

This repository includes an AWS CloudFormation template that creates a Lambda function which polls the Management API, and which you can configure with a Neptune database cluster ID and a polling interval. After it's deployed, this function is named _neptune-endpoint-info_<cluster-id>_. In your application code, you can then create a `ClusterEndpointsRefreshAgent` using the `lambdaProxy()` factory method, supplying the type of endpoint information to be retrieved (for example, `All`, `Primary`, `ReadReplicas`, `ClusterEndpoint` or `ReaderEndpoint`), the name of the proxy Lambda function, and the AWS Region in which the proxy Lambda function is located.

The identity under which you're running your application must be authorized to perform `lambda:Invoke` for your proxy Lambda function.

The following code shows how to create a refresh agent that gets endpoint information from a proxy Lambda function named _neptune-endpoint-info_my-cluster_:

```
ClusterEndpointsRefreshAgent refreshAgent = ClusterEndpointsRefreshAgent.lambdaProxy(
    EndpointsType.ReadReplicas,
    "neptune-endpoint-info_my-cluster",
    "eu-west-1");
	
GremlinCluster cluster = GremlinClusterBuilder.build()
    .enableSsl(true)
    .addContactPoints(refreshAgent.getAddresses().get(EndpointsType.ReadReplicas))
    .port(8182)
    .create();

GremlinClient client = cluster.connect();

refreshAgent.startPollingNeptuneAPI(
    (OnNewAddresses) addresses -> client.refreshEndpoints(addresses.get(EndpointsType.ReadReplicas)),
    60,
    TimeUnit.SECONDS);
```

You can also supply a custom endpoints selector when using the Lambda proxy:

```
EndpointsSelector selector = (clusterEndpoint, readerEndpoint, instances) ->
    instances.stream()
        .filter(NeptuneInstanceMetadata::isReader)
        .filter(i -> i.hasTag("workload", "analytics"))
        .filter(NeptuneInstanceMetadata::isAvailable)
        .map(NeptuneInstanceMetadata::getEndpoint)
        .collect(Collectors.toList());

ClusterEndpointsRefreshAgent refreshAgent = ClusterEndpointsRefreshAgent.lambdaProxy(
    selector,
    "neptune-endpoint-info_my-cluster",
    "eu-west-1");

GremlinCluster cluster = GremlinClusterBuilder.build()
    .enableSsl(true)
    .addContactPoints(refreshAgent.getAddresses().get(selector))
    .port(8182)
    .create();

GremlinClient client = cluster.connect();

refreshAgent.startPollingNeptuneAPI(
    (OnNewAddresses) addresses -> client.refreshEndpoints(addresses.get(selector)),
    60,
    TimeUnit.SECONDS);
```

#### Lambda proxy environment variables

The AWS Lambda proxy has the following environment variables:

  - `clusterId` – The cluster ID of the Amazon Neptune cluster to be polled for endpoint information.
  - `pollingIntervalSeconds` – The number of seconds between polls.
  - `unavailable` – Determines whether specific endpoints will be marked as `unavailable`. Valid values are: `none`, `all`, `writer`, `reader`. 
  
#### Marking specific endpoints as unavailable

The AWS Lambda proxy has an `unavailable` environment variable that accepts the following values: `none`, `all`, `writer`, `reader`. You can use this environment variable to set the status of specific types of endpoint to `unavailable` – even if the Management API says they are available. In this way, you can apply back pressure in the client, preventing it from sending queries while you make changes to your Neptune cluster. To manage this back pressure, your application will have to handle two different kinds of exception – see below. The endpoints are only marked as `unavailable` in the responses returned from the Lambda proxy: the real endpoints remain unaffected.

For example, if you set the `unavailable` environment variable to `reader`, then all read replica endpoints will be marked as `unavailable`. The following selector will, therefore, return an empty list of endpoints:

```
EndpointsSelector availableReadReplicasSelector = (clusterEndpoint, readerEndpoint, instances) ->
    instances.stream()
        .filter(NeptuneInstanceMetadata::isReader)
        .filter(NeptuneInstanceMetadata::isAvailable)
        .map(NeptuneInstanceMetadata::getEndpoint)
        .collect(Collectors.toList());
```

An empty list of endpoints can trigger two different exceptions in the Neptune Gremlin Client:

  - If you try to build a new cluster (using `NeptuneGremlinClusterBuilder` or `GremlinClusterBuilder`) with an empty list of endpoints, the builders's `create()` method will throw an `IllegalArgumentException` with the following message: "The list of endpoint addresses is empty. You must supply one or more endpoints."
  – If an existing cluster is refreshed with an empty list of endpoints, the next query will trigger a `TimeoutException` with the following message: "Timed-out waiting for connection".


#### Installing the neptune-endpoints-info AWS Lambda function

  1. Build the AWS Lambda proxy from [source](https://github.com/awslabs/amazon-neptune-tools/tree/master/neptune-gremlin-client/neptune-endpoints-info-lambda) and put it an Amazon S3 bucket. 
  2. Install the Lambda proxy in your account using [this CloudFormation template](https://github.com/awslabs/amazon-neptune-tools/blob/master/neptune-gremlin-client/cloudformation-templates/neptune-endpoints-info-lambda.json). The template includes parameters for the current Neptune cluster ID, and the S3 source for the Lambda proxy jar (from step 1).
  3. Ensure all parts of your application are using the latest Gremlin Client for Amazon Neptune to connect to and query Neptune.
  4. The Gremlin Client for Amazon Neptune should be configured to fetch the cluster topology information from the Lambda proxy using the `ClusterEndpointsRefreshAgent.lambdaProxy()` method, as per the [examples above](https://github.com/awslabs/amazon-neptune-tools/tree/master/neptune-gremlin-client#connect-the-clusterendpointsrefreshagent-to-a-lambda-proxy-when-you-have-many-clients).

## GremlinClusterBuilder and NeptuneGremlinClusterBuilder

The library includes two cluster builders: `GremlinClusterBuilder` and `NeptuneGremlinClusterBuilder`. `GremlinClusterBuilder` should work with any Gremlin server. You can also use `GremlinClusterBuilder` if you don't use IAM database authentication or a load balancer with your Neptune database.

If you do use IAM database authentication and/or a network or application load balancer, use the `NeptuneGremlinClusterBuilder` instead. This includes additional builder methods for enabling IAM auth (i.e. Sig4 signing of requests) and adding load balancer endpoints and port details:

```
GremlinCluster cluster = NeptuneGremlinClusterBuilder.build()
        .enableSsl(true)
        .enableIamAuth(true)
        .addContactPoint(neptuneClusterEndpoint)
        .networkLoadBalancerEndpoint(networkLoadBalancerEndpoint)
        .create();
```

If you are using a load balancer or a proxy server (such as HAProxy), you must use SSL termination and have your own SSL certificate on the proxy server. SSL passthrough doesn't work because the provided SSL certificates don't match the proxy server hostname. Note that the Network Load Balancer, although a Layer 4 load balancer, now supports [TLS termination](https://aws.amazon.com/blogs/aws/new-tls-termination-for-network-load-balancers/).

### Credentials

When the Gremlin Client for Amazon Neptune connects to an IAM auth enabled database it uses a `DefaultAWSCredentialsProviderChain` to supply credentials to the signing process. You can modify this behavior in a couple of different ways.

To customize which profile is sourced from a local credentials file, use the `iamProfile()` method: 

```
GremlinCluster cluster = NeptuneGremlinClusterBuilder.build()
        .enableSsl(true)
        .enableIamAuth(true)
        .addContactPoint(neptuneClusterEndpoint)
        .networkLoadBalancerEndpoint(networkLoadBalancerEndpoint)
        .iamProfile(profileName)
        .create();
```

Or you can supply your own `AwsCredentialsProvider`:

```
GremlinCluster cluster = NeptuneGremlinClusterBuilder.build()
        .enableSsl(true)
        .enableIamAuth(true)
        .addContactPoint(neptuneClusterEndpoint)
        .networkLoadBalancerEndpoint(networkLoadBalancerEndpoint)
        .credentials(new ProfileCredentialsProvider(profileName))
        .create();
```

The client includes a load balancer-aware handshake interceptor that will [sign requests and adjust HTTP headers as necessary](https://github.com/aws-samples/aws-dbs-refarch-graph/tree/master/src/connecting-using-a-load-balancer). However, you can replace this interceptor with your own implementation:

```
GremlinCluster cluster = NeptuneGremlinClusterBuilder.build()
        .enableSsl(true)
				.enableIamAuth(true)
        .addContactPoint(neptuneClusterEndpoint)
        .networkLoadBalancerEndpoint(networkLoadBalancerEndpoint)
        .handshakeInterceptor(r -> { 
              NeptuneNettyHttpSigV4Signer sigV4Signer = new NeptuneNettyHttpSigV4Signer(serviceRegion, new DefaultAWSCredentialsProviderChain());
              sigV4Signer.signRequest(r);
              return r;
        })
        .create();
```

### Service region

If you have IAM database authentication enabled for your Neptune database, you _must_ specify the Neptune service region when connecting from your client.

By default, the Gremlin Client for Amazon Neptune will attempt to source this region parameter from several different places:

  - The **SERVICE_REGION** environment variable.
  - The **SERVICE_REGION** system property.
  - The **AWS_REGION** Lambda environment variable (this assumes Neptune is in the same region as teh Lambda function).
  - Using the `Regions.getCurrentRegion()` method from the [AWS SDK for Java](https://aws.amazon.com/blogs/developer/determining-an-applications-current-region/) (this assumes Neptune is in the current region).

You can also specify the service region when creating a `GremlinCluster` using the `NeptuneGremlinClusterBuilder.serviceRegion()` method:

```
GremlinCluster cluster = NeptuneGremlinClusterBuilder.build()
        .enableSsl(true)
        .enableIamAuth(true)
        .addContactPoint(neptuneClusterEndpoint)
        .networkLoadBalancerEndpoint(networkLoadBalancerEndpoint)
        .serviceRegion(regionName)
        .create();
```

### Connection timeouts and refreshing endpoint addresses after connection failures

Whenever you submit a Gremlin request to a `GremlinClient`, the client repeatedly tries to acquire a connection until it either succeeds, a `ConnectionException` occurs, or a timeout threshold is exceeded.

As of version 1.0.2, the `GremlinClient.chooseConnection()` method now respects the `maxWaitForConnection` value specified when you create a `GremlinCluster`. The following example creates a `GremlinClient` whose `chooseConnection()` method will throw a `TimeoutException` after 10 seconds if it can't acquire a connection.

```
GremlinCluster cluster = GremlinClusterBuilder.build()
        .maxWaitForConnection(10000)
        ...
        .create();

GremlinClient client = cluster.connect();
```

If you don't specify a `maxWaitForConnection` value, the `GremlinCluster` uses a default value of 16,000 milliseconds.

Whenever a `GremlinClient` attempts to acquire a connection, it iterates through the connection pools associated with the endpoints with which it has been configured, looking for the first healthy connection. It waits 5 milliseconds between attempts to get a connection.

Using `GremlinClusterBuilder.refreshOnErrorThreshold()` and `GremlinClusterBuilder.refreshOnErrorEventHandler()` (and the `NeptuneGremlinClusterBuilder` equivalents), you can instruct a `GremlinClinet` to refresh its endpoints after a certain number of consecutive failures to acquire a connection. The following example shows how to create a `GremlinClient` that will refresh its endpoints after 1000 consecutive failures to acquire a connection:

```
EndpointsType selector = EndpointsType.ReadReplicas;

ClusterEndpointsRefreshAgent refreshAgent = new ClusterEndpointsRefreshAgent(
    clusterId,
    selector);

GremlinCluster cluster = GremlinClusterBuilder.build()
        .addContactPoints(refreshAgent.getAddresses().get(EndpointsType.ReadReplicas))
        .refreshOnErrorThreshold(1000)
        .refreshOnErrorEventHandler(() -> refreshAgent.getAddresses().get(selector))
        .maxWaitForConnection(20000)
        .create();

GremlinClient client = cluster.connect();

refreshAgent.startPollingNeptuneAPI(
    (OnNewAddresses) addresses -> client.refreshEndpoints(addresses.get(selector)),
    60,
    TimeUnit.SECONDS);
```

The `refreshAgent` here is configured to find the endpoint addresses of all database instances in a Neptune cluster that are currently acting as readers. The `GremlinClusterBuilder` creates a `GremlinCluster` whose contact points (i.e. its endpoint addresses) are initialized via a first invocation of the `refreshAgent`. But the builder also configures the cluster so that after 1000 consecutive failed attempts to acquire a connection from its currently configured endpoint addresses, it refreshes those addresses using the agent. The cluster is also configured to timeout attempts to get a connection after 20 seconds. At the end of the snippet we also configure the `refreshAgent` to refresh the `GremlinClient` every minute, irrespective of any failures or successes.

With this setup, then, the `GremlinClient` will refresh its endpoint addresses once every minute. It will also refresh its endpoints after 1000 consecutive failed attempts to get a connection. If any attempt to get a connection takes longer than 20 seconds, the client will throw a `TimeoutException`.

### Transactions

The Gremlin Client for Amazon Neptune support executing Gremlin transactions, as long as the transactions are issued against a writer endpoint:

```
EndpointsType selector = EndpointsType.ClusterEndpoint;

ClusterEndpointsRefreshAgent refreshAgent = new ClusterEndpointsRefreshAgent(
    "my-cluster-id",
    selector);
	
GremlinCluster cluster = GremlinClusterBuilder.build()
    .enableSsl(true)
    .addContactPoints(refreshAgent.getAddresses().get(selector))
    .create();
    
GremlinClient client = cluster.connect();

refreshAgent.startPollingNeptuneAPI(
    (OnNewAddresses) addresses -> client.refreshEndpoints(addresses.get(selector)),
    60,
    TimeUnit.SECONDS);

DriverRemoteConnection connection = DriverRemoteConnection.using(client);

Transaction tx = traversal().withRemote(connection).tx();

GraphTraversalSource g = tx.begin();

try {

    String id1 = UUID.randomUUID().toString();
    String id2 = UUID.randomUUID().toString();
    
    g.addV("testNode").property(T.id, id1).iterate();
    g.addV("testNode").property(T.id, id2).iterate();
    g.addE("testEdge").from(__.V(id1)).to(__.V(id2)).iterate();
    
    tx.commit();

} catch (Exception e) {
    tx.rollback();
}


refreshAgent.close();
client.close();
cluster.close();
```

If you attempt to issue transactions against a read replica, the client returns an error:

```
org.apache.tinkerpop.gremlin.driver.exception.ResponseException: {"detailedMessage":"Gremlin update operation attempted on a read-only replica.","requestId":"05074a8e-c9ef-42b7-9f3e-1c388cd35ae0","code":"ReadOnlyViolationException"}
```

## Demos
 
The demo includes three sample scenarios:
 
### RollingSubsetOfEndpointsDemo
 
Create a Neptune cluster with 2 or more instances and supply the instance endpoints to the demo using the `--endpoint` parameter:
 
```
java -jar gremlin-client-demo.jar rolling-endpoints-demo \
  --enable-ssl \
  --endpoint <my-primary-endpoint> \
  --endpoint <my-replica-endpoint-1> \
  --endpoint <my-replica-endpoint-2>
```
 
When the demo runs, the `GremlinClient` is initialized with approximately two thirds of the endpoints in the cluster (1 endpoint in a 2 instance cluster, 2 endpoints in a 3 instance cluster, 3 endpoints in a 5 instance cluster, etc). The demo repeatedly issues simple queries against the cluster via these endpoints. (It doesn't do anything with the query results – you just see some feedback every 10,000 queries.) Every minute, the demo rolls the endpoints to use a different subset of endpoints.
 
You can view the distribution of queries across the cluster using the Gremlin Requests/Sec CloudWatch metric from the Gremlin console.
 
### RefreshAgentDemo
 
This demo uses a `ClusterTopologyRefreshAgent` to query the Neptune APIs for the current cluster topology every 15 seconds. The `GremlinClient` adapts accordingly.
 
The command for this demo requires a `--cluster-id` parameter. The identity under which you're running the demo must be authorized to perform `rds:DescribeDBClusters`,  `rds:DescribeDBInstances` and `rds:ListTagsForResource` for your Neptune cluster.
 
```
java -jar gremlin-client-demo.jar refresh-agent-demo \
  --enable-ssl \
  --cluster-id <my-cluster-id>
```
 
With this demo, try triggering a failover in the cluster. After approx 15 seconds you should see a new endpoint added to the client, and the old endpoint removed. While the failover is occurring, you may see some queries fail: I've used a simple exception handler to log these errors.

### TxDemo
 
This demo demonstrates using the Neptune Gremlin client to issue transactions.
 
The command for this demo requires a `--cluster-id` parameter. The identity under which you're running the demo must be authorized to perform `rds:DescribeDBClusters`,  `rds:DescribeDBInstances` and `rds:ListTagsForResource` for your Neptune cluster.
 
```
java -jar gremlin-client-demo.jar tx-demo \
  --enable-ssl \
  --cluster-id <my-cluster-id>
```

 