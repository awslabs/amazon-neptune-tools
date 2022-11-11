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

package software.amazon.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.evanlennick.retry4j.CallExecutor;
import com.evanlennick.retry4j.CallExecutorBuilder;
import com.evanlennick.retry4j.Status;
import com.evanlennick.retry4j.config.RetryConfig;
import com.evanlennick.retry4j.config.RetryConfigBuilder;
import org.apache.tinkerpop.gremlin.driver.GremlinClient;
import org.apache.tinkerpop.gremlin.driver.GremlinCluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnectionException;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import software.amazon.neptune.cluster.*;

import java.io.*;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.addV;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.unfold;

public class NeptuneGremlinClientExampleLambda implements RequestStreamHandler {

    private static final EndpointsType ENDPOINT_TYPE = EndpointsType.ClusterEndpoint;

    private final ClusterEndpointsRefreshAgent refreshAgent;
    private final GremlinClient client;
    private final GraphTraversalSource g;
    private final CallExecutor<Object> executor;

    private final Random idGenerator = new Random();

    public NeptuneGremlinClientExampleLambda() {

        this.refreshAgent = ClusterEndpointsRefreshAgent.lambdaProxy(
                ENDPOINT_TYPE,
                System.getenv("neptuneEndpointsInfoLambda"),
                System.getenv("AWS_REGION"));

        GremlinCluster cluster = NeptuneGremlinClusterBuilder.build()
                .enableSsl(true)
                .addContactPoints(refreshAgent.getAddresses().get(ENDPOINT_TYPE))
                .port(8182)
                .serializer(Serializers.GRAPHBINARY_V1D0)
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(1)
                .create();

        this.client = cluster.connect();

        refreshAgent.startPollingNeptuneAPI(
                (OnNewAddresses) addresses -> client.refreshEndpoints(addresses.get(ENDPOINT_TYPE)),
                5,
                TimeUnit.SECONDS);

        this.g = AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using(client));

        RetryConfig retryConfig = new RetryConfigBuilder()
                .retryOnCustomExceptionLogic(retryLogic())
                .withDelayBetweenTries(2000, ChronoUnit.MILLIS)
                .withMaxNumberOfTries(10)
                .withExponentialBackoff()
                .build();


        this.executor = new CallExecutorBuilder<Object>()
                .config(retryConfig)
                .afterFailedTryListener(this::afterFailedTry)
                .build();

    }

    @Override
    public void handleRequest(InputStream input,
                              OutputStream output,
                              Context context) throws IOException {

        try {

            String id = String.valueOf(idGenerator.nextInt());

            @SuppressWarnings("unchecked")
            Callable<Object> query = () -> g.V(id)
                    .fold()
                    .coalesce(
                            unfold(),
                            addV("Person").property(T.id, id))
                    .id().next();

            Status<Object> status = executor.execute(query);

            try (Writer writer = new BufferedWriter(new OutputStreamWriter(output, UTF_8))) {
                writer.write(status.getResult().toString());
            }

        } finally {
            input.close();
            output.close();
        }
    }

    private Function<Exception, Boolean> retryLogic() {

        return e -> {

            StringWriter stringWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stringWriter));
            String message = stringWriter.toString();

            Class<? extends Exception> exceptionClass = e.getClass();

            if (RemoteConnectionException.class.isAssignableFrom(exceptionClass)) {
                System.out.println("Retrying because RemoteConnectionException");
                return true;
            }

            // Check for connection issues
            if (message.contains("Timed out while waiting for an available host") ||
                    message.contains("Timed-out waiting for connection on Host") ||
                    message.contains("Connection to server is no longer active") ||
                    message.contains("Connection reset by peer") ||
                    message.contains("SSLEngine closed already") ||
                    message.contains("Pool is shutdown") ||
                    message.contains("ExtendedClosedChannelException") ||
                    message.contains("Broken pipe")) {
                return true;
            }

            // Concurrent writes can sometimes trigger a ConcurrentModificationException.
            // In these circumstances you may want to backoff and retry.
            if (message.contains("ConcurrentModificationException")) {
                return true;
            }

            // If the primary fails over to a new instance, existing connections to the old primary will
            // throw a ReadOnlyViolationException. You may want to back and retry.
            if (message.contains("ReadOnlyViolationException")) {
                return true;
            }

            return false;
        };
    }

    private void afterFailedTry(Status<?> status) {
        // If the primary fails over to a new instance, existing connections to the old primary will
        // throw a ReadOnlyViolationException. While the client is backing off, you may want to
        // refresh the endpoint addresses.
        if (status.getLastExceptionThatCausedRetry().getMessage().contains("ReadOnlyViolationException")) {
            client.refreshEndpoints(refreshAgent.getAddresses().get(ENDPOINT_TYPE));
        }
    }
}
