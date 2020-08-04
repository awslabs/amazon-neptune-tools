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

import software.amazon.neptune.cluster.NeptuneGremlinClusterBuilder;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Port;
import com.github.rvesse.airline.annotations.restrictions.PortType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import org.apache.tinkerpop.gremlin.driver.GremlinClient;
import org.apache.tinkerpop.gremlin.driver.GremlinCluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Command(name = "rolling-endpoints-demo", description = "Demo using rolling set of endpoints with topology aware cluster and client")
public class RollingSubsetOfEndpointDemo implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RollingSubsetOfEndpointDemo.class);

    @Option(name = {"--endpoint"}, description = "Neptune endpoint(s)")
    @Required
    private Queue<String> endpoints = new LinkedList<>();

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

    @Option(name = {"--query-count"}, description = "Number of queries to execute")
    @Once
    private int queryCount = 1000000;

    @Option(name = {"--log-level"}, description = "Log level")
    @Once
    private String logLevel = "info";

    @Option(name = {"--interval"}, description = "Interval (in seconds) between rolling endpoints")
    @Once
    private int intervalSeconds = 60;


    @Override
    public void run() {

        try {

            GremlinCluster cluster = NeptuneGremlinClusterBuilder.build()
                    .enableSsl(enableSsl)
                    .enableIamAuth(enableIam)
                    .addContactPoints(nextSetOfAddresses())
                    .port(neptunePort)
                    .create();

            GremlinClient client = cluster.connect();

            ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                client.refreshEndpoints(nextSetOfAddresses());
            }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);

            DriverRemoteConnection connection = DriverRemoteConnection.using(client);
            GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(connection);

            for (int i = 0; i < queryCount; i++) {
                try {
                    List<Map<Object, Object>> results = g.V().limit(10).valueMap(true).toList();
                    for (Map<Object, Object> result : results) {
                        //Do nothing
                    }
                    if (i % 10000 == 0) {
                        System.out.println();
                        System.out.println("Number of queries: " + i);
                    }
                } catch (Exception e) {
                    logger.warn("Error processing query: {}", e.getMessage());
                }
            }

            scheduledExecutorService.shutdownNow();

            client.close();
            cluster.close();

        } catch (Exception e) {
            System.err.println("An error occurred while connecting to Neptune:");
            e.printStackTrace();
            System.exit(-1);
        }

    }

    public int approxTwoThirds(int i) {
        int v = (int) Math.floor(((double) 2 / (double) 3) * i);
        return v == 0 ? 1 : v;
    }

    public Collection<String> nextSetOfAddresses() {
        List<String> currentAddresses = new ArrayList<>();
        for (int i = 0; i < approxTwoThirds(endpoints.size()); i++) {
            String a = endpoints.poll();
            currentAddresses.add(a);
            endpoints.offer(a);
        }
        return currentAddresses;
    }
}
