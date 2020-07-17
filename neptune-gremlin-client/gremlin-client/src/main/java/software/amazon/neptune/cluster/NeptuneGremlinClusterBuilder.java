/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package software.amazon.neptune.cluster;

import io.netty.handler.ssl.SslContext;
import org.apache.tinkerpop.gremlin.driver.*;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public class NeptuneGremlinClusterBuilder {

    public static NeptuneGremlinClusterBuilder build() {
        return new NeptuneGremlinClusterBuilder();
    }

    private final GremlinClusterBuilder innerBuilder = GremlinClusterBuilder.build();

    private List<String> addresses = new ArrayList<>();
    private String networkLoadBalancerEndpoint;
    private String applicationLoadBalancerEndpoint;
    private boolean enableIamAuth = false;
    private int port = 8182;
    private int loadBalancerPort = 80;

    private NeptuneGremlinClusterBuilder() {
    }

    public NeptuneGremlinClusterBuilder refreshOnErrorThreshold(final int refreshOnErrorThreshold) {
        innerBuilder.refreshOnErrorThreshold(refreshOnErrorThreshold);
        return this;
    }

    public NeptuneGremlinClusterBuilder refreshOnErrorEventHandler(final Supplier<Collection<String>> refreshOnErrorEventHandler) {
        innerBuilder.refreshOnErrorEventHandler(refreshOnErrorEventHandler);
        return this;
    }

    public NeptuneGremlinClusterBuilder nioPoolSize(final int nioPoolSize) {
        innerBuilder.nioPoolSize(nioPoolSize);
        return this;
    }

    public NeptuneGremlinClusterBuilder workerPoolSize(final int workerPoolSize) {
        innerBuilder.workerPoolSize(workerPoolSize);
        return this;
    }

    public NeptuneGremlinClusterBuilder path(final String path) {
        innerBuilder.path(path);
        return this;
    }

    public NeptuneGremlinClusterBuilder serializer(final String mimeType) {
        innerBuilder.serializer(mimeType);
        return this;
    }

    public NeptuneGremlinClusterBuilder serializer(final Serializers mimeType) {
        innerBuilder.serializer(mimeType);
        return this;
    }

    public NeptuneGremlinClusterBuilder serializer(final MessageSerializer serializer) {
        innerBuilder.serializer(serializer);
        return this;
    }

    public NeptuneGremlinClusterBuilder enableSsl(final boolean enable) {
        innerBuilder.enableSsl(enable);
        return this;
    }

    public NeptuneGremlinClusterBuilder sslContext(final SslContext sslContext) {
        innerBuilder.sslContext(sslContext);
        return this;
    }

    public NeptuneGremlinClusterBuilder keepAliveInterval(final long keepAliveInterval) {
        innerBuilder.keepAliveInterval(keepAliveInterval);
        return this;
    }

    public NeptuneGremlinClusterBuilder keyStore(final String keyStore) {
        innerBuilder.keyStore(keyStore);
        return this;
    }

    public NeptuneGremlinClusterBuilder keyStorePassword(final String keyStorePassword) {
        innerBuilder.keyStorePassword(keyStorePassword);
        return this;
    }

    public NeptuneGremlinClusterBuilder trustStore(final String trustStore) {
        innerBuilder.trustStore(trustStore);
        return this;
    }

    public NeptuneGremlinClusterBuilder trustStorePassword(final String trustStorePassword) {
        innerBuilder.trustStorePassword(trustStorePassword);
        return this;
    }

    public NeptuneGremlinClusterBuilder keyStoreType(final String keyStoreType) {
        innerBuilder.keyStoreType(keyStoreType);
        return this;
    }

    public NeptuneGremlinClusterBuilder sslEnabledProtocols(final List<String> sslEnabledProtocols) {
        innerBuilder.sslEnabledProtocols(sslEnabledProtocols);
        return this;
    }

    public NeptuneGremlinClusterBuilder sslCipherSuites(final List<String> sslCipherSuites) {
        innerBuilder.sslCipherSuites(sslCipherSuites);
        return this;
    }

    public NeptuneGremlinClusterBuilder sslSkipCertValidation(final boolean sslSkipCertValidation) {
        innerBuilder.sslSkipCertValidation(sslSkipCertValidation);
        return this;
    }

    public NeptuneGremlinClusterBuilder minInProcessPerConnection(final int minInProcessPerConnection) {
        innerBuilder.minInProcessPerConnection(minInProcessPerConnection);
        return this;
    }

    public NeptuneGremlinClusterBuilder maxInProcessPerConnection(final int maxInProcessPerConnection) {
        innerBuilder.maxInProcessPerConnection(maxInProcessPerConnection);
        return this;
    }

    public NeptuneGremlinClusterBuilder maxSimultaneousUsagePerConnection(final int maxSimultaneousUsagePerConnection) {
        innerBuilder.maxSimultaneousUsagePerConnection(maxSimultaneousUsagePerConnection);
        return this;
    }

    public NeptuneGremlinClusterBuilder minSimultaneousUsagePerConnection(final int minSimultaneousUsagePerConnection) {
        innerBuilder.minSimultaneousUsagePerConnection(minSimultaneousUsagePerConnection);
        return this;
    }

    public NeptuneGremlinClusterBuilder maxConnectionPoolSize(final int maxSize) {
        innerBuilder.maxConnectionPoolSize(maxSize);
        return this;
    }

    public NeptuneGremlinClusterBuilder minConnectionPoolSize(final int minSize) {
        innerBuilder.minConnectionPoolSize(minSize);
        return this;
    }

    public NeptuneGremlinClusterBuilder resultIterationBatchSize(final int size) {
        innerBuilder.resultIterationBatchSize(size);
        return this;
    }

    public NeptuneGremlinClusterBuilder maxWaitForConnection(final int maxWait) {
        innerBuilder.maxWaitForConnection(maxWait);
        return this;
    }

    public NeptuneGremlinClusterBuilder maxWaitForSessionClose(final int maxWait) {
        innerBuilder.maxWaitForSessionClose(maxWait);
        return this;
    }

    public NeptuneGremlinClusterBuilder maxContentLength(final int maxContentLength) {
        innerBuilder.maxContentLength(maxContentLength);
        return this;
    }

    public NeptuneGremlinClusterBuilder channelizer(final String channelizerClass) {
        innerBuilder.channelizer(channelizerClass);
        return this;
    }

    public NeptuneGremlinClusterBuilder channelizer(final Class channelizerClass) {
        return channelizer(channelizerClass.getCanonicalName());
    }

    public NeptuneGremlinClusterBuilder validationRequest(final String script) {
        innerBuilder.validationRequest(script);
        return this;
    }

    public NeptuneGremlinClusterBuilder reconnectInterval(final int interval) {
        innerBuilder.reconnectInterval(interval);
        return this;
    }

    public NeptuneGremlinClusterBuilder loadBalancingStrategy(final Supplier<LoadBalancingStrategy> loadBalancingStrategy) {
        innerBuilder.loadBalancingStrategy(loadBalancingStrategy);
        return this;
    }

    public NeptuneGremlinClusterBuilder authProperties(final AuthProperties authProps) {
        innerBuilder.authProperties(authProps);
        return this;
    }

    public NeptuneGremlinClusterBuilder credentials(final String username, final String password) {
        innerBuilder.credentials(username, password);
        return this;
    }

    public NeptuneGremlinClusterBuilder protocol(final String protocol) {
        innerBuilder.protocol(protocol);
        return this;
    }

    public NeptuneGremlinClusterBuilder jaasEntry(final String jaasEntry) {
        innerBuilder.jaasEntry(jaasEntry);
        return this;
    }

    public NeptuneGremlinClusterBuilder addContactPoint(final String address) {
        this.addresses.add(address);
        return this;
    }

    public NeptuneGremlinClusterBuilder addContactPoints(final String... addresses) {
        for (String address : addresses)
            addContactPoint(address);
        return this;
    }

    public NeptuneGremlinClusterBuilder addContactPoints(final Collection<String> addresses) {
        for (String address : addresses)
            addContactPoint(address);
        return this;
    }

    public NeptuneGremlinClusterBuilder port(final int port) {
        this.port = port;
        return this;
    }

    public NeptuneGremlinClusterBuilder loadBalancerPort(final int port) {
        this.loadBalancerPort = port;
        return this;
    }

    public NeptuneGremlinClusterBuilder networkLoadBalancerEndpoint(final String endpoint) {
        this.networkLoadBalancerEndpoint = endpoint;
        return this;
    }

    public NeptuneGremlinClusterBuilder applicationLoadBalancerEndpoint(final String endpoint) {
        this.applicationLoadBalancerEndpoint = endpoint;
        return this;
    }

    public NeptuneGremlinClusterBuilder enableIamAuth(final boolean enable) {
        this.enableIamAuth = enable;
        return this;
    }

    private boolean isDirectConnection() {
        return networkLoadBalancerEndpoint == null && applicationLoadBalancerEndpoint == null;
    }

    public GremlinCluster create() {

        if (addresses.isEmpty()) {
            if (isDirectConnection()) {
                throw new IllegalArgumentException("You must supply one or more Neptune endpoints");
            } else if (enableIamAuth) {
                throw new IllegalArgumentException("You must supply one or more Neptune endpoints to sign the Host header");
            }
        }

        if (isDirectConnection()) {
            innerBuilder.port(port);
            for (String address : addresses) {
                innerBuilder.addContactPoint(address);
            }
        } else {
            innerBuilder.port(loadBalancerPort);
            if (networkLoadBalancerEndpoint != null) {
                innerBuilder.addContactPoint(networkLoadBalancerEndpoint);
            } else if (applicationLoadBalancerEndpoint != null) {
                innerBuilder.addContactPoint(applicationLoadBalancerEndpoint);
            }
        }

        if (enableIamAuth) {

            if (isDirectConnection()) {
                innerBuilder.channelizer(SigV4WebSocketChannelizer.class);
            } else {

                HandshakeRequestConfig.HandshakeRequestConfigBuilder handshakeRequestConfigBuilder =
                        HandshakeRequestConfig.builder()
                                .addNeptuneEndpoints(addresses)
                                .setNeptunePort(port);

                if (applicationLoadBalancerEndpoint != null) {
                    handshakeRequestConfigBuilder.removeHostHeaderAfterSigning();
                }

                HandshakeRequestConfig handshakeRequestConfig = handshakeRequestConfigBuilder.build();

                innerBuilder
                        // We're using the JAAS_ENTRY auth property to tunnel Host header info to the channelizer
                        .jaasEntry(handshakeRequestConfig.value())
                        .channelizer(LBAwareSigV4WebSocketChannelizer.class);
            }
        }

        return innerBuilder.create();
    }
}
