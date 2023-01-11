# Gremlin Client for Amazon Neptune

A Java Gremlin client for Amazon Neptune that allows you to change the endpoints used by the client as it is running. Includes an agent that can query the Amazon Neptune API for cluster details, and update the client on a periodic basis. You can supply your own custom endpoint selectors to configure the client for a subset of instances in your cluster based on tags, instance types, instance IDs, AZs, etc.

The client also provides support for IAM database authentication, and for connecting to Neptune via a network or application load balancer.

If your application uses a lot of concurrent clients, you should proxy endpoint refresh requests through a Lambda function that periodically queries the Management API and then caches the results on behalf of your clients. This repository includes an AWS Lambda function that can act as a Neptune endpoints information proxy.

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
GremlinCluster cluster = NeptuneGremlinClusterBuilder.build()
        .enableSsl(true)
				.enableIamAuth(true)
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

## ClusterEndpointsRefreshAgent

The `ClusterEndpointsRefreshAgent` allows you to schedule endpoint updates to a client based on a Neptune cluster ID.  The identity under which you're running the agent must be authorized to perform `rds:DescribeDBClusters`,  `rds:DescribeDBInstances` and `rds:ListTagsForResource` for your Neptune cluster.

The following shows how to refresh a client with a cluster's available replica endpoints every 60 seconds:

```
EndpointsType selector = EndpointsType.ReadReplicas;
            
ClusterEndpointsRefreshAgent refreshAgent = new ClusterEndpointsRefreshAgent(
        clusterId,
        selector);

GremlinCluster cluster = GremlinClusterBuilder.build()
        .enableSsl
				.enableIamAuth(true)
        .addContactPoints(refreshAgent.getAddresses().get(selector))
        .port(8182)
        .create();

GremlinClient client = cluster.connect();

refreshAgent.startPollingNeptuneAPI(
        (OnNewAddresses) addresses -> client.refreshEndpoints(addresses.get(selector)),
        60,
        TimeUnit.SECONDS);
```

### EndpointsSelector

The `ClusterEndpointsRefreshAgent` constructor accepts an `EndpointsSelector` that allows you to add custom endpoint selection logic. The following example shows how to select endpoints for all **Available** instances with a **workload** tag whose value is **analytics**:

```
EndpointsSelector selector = (clusterEndpoint, readerEndpoint, instances) ->
    instances.stream()
        .filter(NeptuneInstanceProperties::isReader)
        .filter(i -> i.hasTag("workload", "analytics"))
        .filter(NeptuneInstanceProperties::isAvailable)
        .map(NeptuneInstanceProperties::getEndpoint)
        .collect(Collectors.toList());

ClusterEndpointsRefreshAgent refreshAgent = new ClusterEndpointsRefreshAgent(
    clusterId,
    selector);

GremlinCluster cluster = GremlinClusterBuilder.build()
    .enableSsl(true)
		.enableIamAuth(true)
    .addContactPoints(refreshAgent.getAddresses())
    .port(8182)
    .create();

GremlinClient client = cluster.connect();

refreshAgent.startPollingNeptuneAPI(
    (OnNewAddresses) addresses -> client.refreshEndpoints(addresses.get(selector)),
    60,
    TimeUnit.SECONDS);
```

The `EndpointsType` enum provides implementations of `EndpointsSelector` for some common use cases:

  * `EndpointsType.All` –  return all available instance (primary and read replicas) endpoints
  * `EndpointsType.Primary` – returns the primary instance endpoint if it is available, or the cluster endpoint if the primary instance endpoint is not available
  * `EndpointsType.ReadReplicas` – returns all available read replica instance endpoints, or, if there are no replica instance endpoints, the reader endpoint
  * `EndpointsType.ClusterEndpoint` – returns the cluster endpoint
  * `EndpointsType.ReaderEndpoint` – return the reader endpoint

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
		.enableIamAuth(true)
    .addContactPoints(refreshAgent.getAddresses())
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
        .filter(NeptuneInstanceProperties::isReader)
        .filter(i -> i.hasTag("workload", "analytics"))
        .filter(NeptuneInstanceProperties::isAvailable)
        .map(NeptuneInstanceProperties::getEndpoint)
        .collect(Collectors.toList());

ClusterEndpointsRefreshAgent refreshAgent = ClusterEndpointsRefreshAgent.lambdaProxy(
    selector,
    "neptune-endpoint-info_my-cluster",
    "eu-west-1");

GremlinCluster cluster = GremlinClusterBuilder.build()
    .enableSsl(true)
		.enableIamAuth(true)
    .addContactPoints(refreshAgent.getAddresses())
    .port(8182)
    .create();

GremlinClient client = cluster.connect();

refreshAgent.startPollingNeptuneAPI(
    (OnNewAddresses) addresses -> client.refreshEndpoints(addresses.get(selector)),
    60,
    TimeUnit.SECONDS);
```

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

ClusterEndpointsRefreshAgent refreshAgent = new ClusterEndpointsRefreshAgent(
    clusterId,
    EndpointsType.ReadReplicas);

GremlinCluster cluster = GremlinClusterBuilder.build()
        .addContactPoints(refreshAgent.getAddresses().get(EndpointsType.ReadReplicas))
        .refreshOnErrorThreshold(1000)
        .refreshOnErrorEventHandler(refreshAgent.getAddresses().get(EndpointsType.ReadReplicas))
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
    'my-cluster-id',
    selector);
	
GremlinCluster cluster = GremlinClusterBuilder.build()
    .enableSsl(true)
    .enableIamAuth(true)
    .addContactPoints(refreshAgent.getAddresses())
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

 