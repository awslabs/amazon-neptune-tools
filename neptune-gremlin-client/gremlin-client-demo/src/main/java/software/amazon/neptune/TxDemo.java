/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package software.amazon.neptune;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Port;
import com.github.rvesse.airline.annotations.restrictions.PortType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.driver.GremlinClient;
import org.apache.tinkerpop.gremlin.driver.GremlinCluster;
import org.apache.tinkerpop.gremlin.driver.IamAuthConfig;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.neptune.cluster.*;
import software.amazon.utils.RegionUtils;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

@Command(name = "tx-demo", description = "Transactional writes demo using the Neptune Gremlin Client")
public class TxDemo implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TxDemo.class);

    @Option(name = {"--cluster-id"}, description = "Amazon Neptune cluster ID")
    @Once
    @Required
    private String clusterId;

    @Option(name = {"--port"}, description = "Neptune port (optional, default 8182)")
    @Port(acceptablePorts = {PortType.SYSTEM, PortType.USER})
    @Once
    private int neptunePort = 8182;

    @Option(name = {"--enable-ssl"}, description = "Enables connectivity over SSL (optional, default false)")
    @Once
    private boolean enableSsl = false;

    @Option(name = {"--enable-iam"}, description = "Enables IAM database authentication (optional, default false)")
    @Once
    private boolean enableIam = false;

    @Option(name = {"--tx-count"}, description = "Number of transactions to execute")
    @Once
    private int txCount = 10;

    @Option(name = {"--log-level"}, description = "Log level")
    @Once
    private String logLevel = "info";

    @Option(name = {"--profile"}, description = "Credentials profile")
    @Once
    private String profile = IamAuthConfig.DEFAULT_PROFILE;

    @Option(name = {"--service-region"}, description = "Neptune service region")
    @Once
    private String serviceRegion = null;

    @Option(name = {"--interval"}, description = "Interval (in seconds) between refreshing addresses")
    @Once
    private int intervalSeconds = 15;

    @Override
    public void run() {

        try {

            EndpointsSelector endpointsSelector = EndpointsType.ClusterEndpoint;

            GetEndpointsFromNeptuneManagementApi fetchStrategy = new GetEndpointsFromNeptuneManagementApi(
                    clusterId,
                    Collections.singletonList(endpointsSelector),
                    RegionUtils.getCurrentRegionName(),
                    profile
            );

            ClusterEndpointsRefreshAgent refreshAgent = new ClusterEndpointsRefreshAgent(fetchStrategy);

            NeptuneGremlinClusterBuilder builder = NeptuneGremlinClusterBuilder.build()
                    .enableSsl(enableSsl)
                    .enableIamAuth(enableIam)
                    .iamProfile(profile)
                    .addContactPoints(refreshAgent.getAddresses().get(endpointsSelector))
                    .minConnectionPoolSize(3)
                    .maxConnectionPoolSize(3)
                    .port(neptunePort);

            if (StringUtils.isNotEmpty(serviceRegion)) {
                builder = builder.serviceRegion(serviceRegion);
            }

            GremlinCluster cluster = builder.create();
            GremlinClient client = cluster.connect();

            refreshAgent.startPollingNeptuneAPI(
                    (OnNewAddresses) addresses -> client.refreshEndpoints(addresses.get(endpointsSelector)),
                    intervalSeconds,
                    TimeUnit.SECONDS);

            DriverRemoteConnection connection = DriverRemoteConnection.using(client);

            for (int i = 0; i < txCount; i++) {

                Transaction tx = traversal().withRemote(connection).tx();
                GraphTraversalSource g = tx.begin();

                try {

                    String id1 = UUID.randomUUID().toString();
                    String id2 = UUID.randomUUID().toString();

                    g.addV("testNode").property(T.id, id1).iterate();
                    g.addV("testNode").property(T.id, id2).iterate();
                    g.addE("testEdge").from(__.V(id1)).to(__.V(id2)).iterate();

                    tx.commit();

                    System.out.println("Tx complete: " + i);
                    System.out.println("id1        : " + id1);
                    System.out.println("id2        : " + id2);

                } catch (Exception e) {
                    logger.warn("Error processing query: {}", e.getMessage());
                    tx.rollback();
                }
            }

            refreshAgent.close();
            client.close();
            cluster.close();

        } catch (Exception e) {
            System.err.println("An error occurred while connecting to Neptune:");
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
