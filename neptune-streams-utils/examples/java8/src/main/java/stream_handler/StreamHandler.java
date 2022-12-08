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

package stream_handler;

import com.amazonaws.neptune.StreamsRecord;
import com.amazonaws.neptune.StreamsResponse;
import com.amazonaws.neptune.config.CredentialsConfig;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.SigV4WebSocketChannelizer;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import utils.EnvironmentVariablesUtils;

import java.io.IOException;
import java.util.Map;

public class StreamHandler extends AbstractStreamHandler {

    private final GraphTraversalSource g;
    private final Cluster cluster;

    public StreamHandler(String neptuneEndpoint,
                         Integer neptunePort,
                         CredentialsConfig credentialsConfig,
                         Map<String, Object> additionalParams) {
        super(neptuneEndpoint, neptunePort, credentialsConfig, additionalParams);

        this.cluster = createCluster();
        this.g = AnonymousTraversalSource
                .traversal()
                .withRemote(DriverRemoteConnection.using(cluster));
    }

    @Override
    public void handleRecords(StreamsResponse streamsResponse) throws IOException {

        StreamsResponse.LastEventId lastEventId = new StreamsResponse.LastEventId();
        int recordCount = 0;

        for (StreamsRecord record : streamsResponse.getRecords()) {

            String op = record.getOp();

            if (op.equals("ADD")) {
                String id = record.getData().getId();
                String type = record.getData().getType();
                if (type.equals("vl")) {
                    System.out.println(g.V(id).valueMap(true).toList());
                } else if (type.equals("e")) {
                    System.out.println(g.E(id).valueMap(true).toList());
                }
            }

            StreamsRecord.EventId eventId = record.getEventId();

            lastEventId.setCommitNum(eventId.getCommitNum());
            lastEventId.setOpNum(record.getEventId().getOpNum());

            recordCount++;
        }

        streamsResponse.setLastEventId(lastEventId);
        streamsResponse.setTotalRecords(recordCount);

    }

    @Override
    public void close() throws IOException {
        cluster.close();
    }

    private Cluster createCluster() {
        Cluster.Builder builder = Cluster.build()
                .addContactPoint(String.valueOf(additionalParams.get("neptune_cluster_endpoint")))
                .port((int) additionalParams.get("neptune_port"))
                .enableSsl(true)
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(1)
                .serializer(Serializers.GRAPHBINARY_V1D0)
                .reconnectInterval(2000);

        if (Boolean.parseBoolean(EnvironmentVariablesUtils.getOptionalEnv("iam_auth_enabled", "false"))) {
            builder = builder.channelizer(SigV4WebSocketChannelizer.class);
        }

        return builder.create();
    }
}
