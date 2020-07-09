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

package org.apache.tinkerpop.gremlin.driver;

import io.netty.handler.ssl.SslContext;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;

import javax.net.ssl.TrustManager;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class GremlinClusterBuilder {

    public static GremlinClusterBuilder build() {
        return new GremlinClusterBuilder();
    }

    private List<String> addresses = new ArrayList<>();
    private int port = 8182;
    private String path = "/gremlin";
    private MessageSerializer serializer = Serializers.GRAPHBINARY_V1D0.simpleInstance();
    private int nioPoolSize = Runtime.getRuntime().availableProcessors();
    private int workerPoolSize = Runtime.getRuntime().availableProcessors() * 2;
    private int minConnectionPoolSize = ConnectionPool.MIN_POOL_SIZE;
    private int maxConnectionPoolSize = ConnectionPool.MAX_POOL_SIZE;
    private int minSimultaneousUsagePerConnection = ConnectionPool.MIN_SIMULTANEOUS_USAGE_PER_CONNECTION;
    private int maxSimultaneousUsagePerConnection = ConnectionPool.MAX_SIMULTANEOUS_USAGE_PER_CONNECTION;
    private int maxInProcessPerConnection = Connection.MAX_IN_PROCESS;
    private int minInProcessPerConnection = Connection.MIN_IN_PROCESS;
    private int maxWaitForConnection = Connection.MAX_WAIT_FOR_CONNECTION;
    private int maxWaitForSessionClose = Connection.MAX_WAIT_FOR_SESSION_CLOSE;
    private int maxContentLength = Connection.MAX_CONTENT_LENGTH;
    private int reconnectInterval = Connection.RECONNECT_INTERVAL;
    private int resultIterationBatchSize = Connection.RESULT_ITERATION_BATCH_SIZE;
    private long keepAliveInterval = Connection.KEEP_ALIVE_INTERVAL;
    private String channelizer = Channelizer.WebSocketChannelizer.class.getName();
    private boolean enableSsl = false;
    private String trustCertChainFile = null;
    private String keyCertChainFile = null;
    private String keyFile = null;
    private String keyPassword = null;
    private String keyStore = null;
    private String keyStorePassword = null;
    private String trustStore = null;
    private String trustStorePassword = null;
    private String keyStoreType = null;
    private String validationRequest = "''";
    private List<String> sslEnabledProtocols = new ArrayList<>();
    private List<String> sslCipherSuites = new ArrayList<>();
    private boolean sslSkipCertValidation = false;
    private SslContext sslContext = null;
    private Supplier<LoadBalancingStrategy> loadBalancingStrategy = LoadBalancingStrategy.RoundRobin::new;
    private AuthProperties authProps = new AuthProperties();
    private int refreshOnErrorThreshold = -1;
    private Supplier<Collection<String>> refreshOnErrorEventHandler = null;

    private GremlinClusterBuilder() {
    }

    public GremlinClusterBuilder refreshOnErrorThreshold(final int refreshOnErrorThreshold) {
        this.refreshOnErrorThreshold = refreshOnErrorThreshold;
        return this;
    }

    public GremlinClusterBuilder refreshOnErrorEventHandler(final Supplier<Collection<String>> refreshOnErrorEventHandler) {
        this.refreshOnErrorEventHandler = refreshOnErrorEventHandler;
        return this;
    }

    /**
     * Size of the pool for handling request/response operations.  Defaults to the number of available processors.
     */
    public GremlinClusterBuilder nioPoolSize(final int nioPoolSize) {
        this.nioPoolSize = nioPoolSize;
        return this;
    }

    /**
     * Size of the pool for handling background work.  Defaults to the number of available processors multiplied
     * by 2
     */
    public GremlinClusterBuilder workerPoolSize(final int workerPoolSize) {
        this.workerPoolSize = workerPoolSize;
        return this;
    }

    /**
     * The path to the Gremlin service on the host which is "/gremlin" by default.
     */
    public GremlinClusterBuilder path(final String path) {
        this.path = path;
        return this;
    }

    /**
     * Set the {@link MessageSerializer} to use given the exact name of a {@link Serializers} enum.  Note that
     * setting this value this way will not allow specific configuration of the serializer itself.  If specific
     * configuration is required * please use {@link #serializer(MessageSerializer)}.
     */
    public GremlinClusterBuilder serializer(final String mimeType) {
        serializer = Serializers.valueOf(mimeType).simpleInstance();
        return this;
    }

    /**
     * Set the {@link MessageSerializer} to use via the {@link Serializers} enum. If specific configuration is
     * required please use {@link #serializer(MessageSerializer)}.
     */
    public GremlinClusterBuilder serializer(final Serializers mimeType) {
        serializer = mimeType.simpleInstance();
        return this;
    }

    /**
     * Sets the {@link MessageSerializer} to use.
     */
    public GremlinClusterBuilder serializer(final MessageSerializer serializer) {
        this.serializer = serializer;
        return this;
    }

    /**
     * Enables connectivity over SSL - note that the server should be configured with SSL turned on for this
     * setting to work properly.
     */
    public GremlinClusterBuilder enableSsl(final boolean enable) {
        this.enableSsl = enable;
        return this;
    }

    /**
     * Explicitly set the {@code SslContext} for when more flexibility is required in the configuration than is
     * allowed by the {@link GremlinClusterBuilder}. If this value is set to something other than {@code null} then all other
     * related SSL settings are ignored. The {@link #enableSsl} setting should still be set to {@code true} for
     * this setting to take effect.
     */
    public GremlinClusterBuilder sslContext(final SslContext sslContext) {
        this.sslContext = sslContext;
        return this;
    }

    /**
     * File location for a SSL Certificate Chain to use when SSL is enabled. If this value is not provided and
     * SSL is enabled, the default {@link TrustManager} will be used.
     *
     * @deprecated As of release 3.2.10, replaced by {@link #trustStore}
     */
    @Deprecated
    public GremlinClusterBuilder trustCertificateChainFile(final String certificateChainFile) {
        this.trustCertChainFile = certificateChainFile;
        return this;
    }

    /**
     * Length of time in milliseconds to wait on an idle connection before sending a keep-alive request. This
     * setting is only relevant to {@link Channelizer} implementations that return {@code true} for
     * {@link Channelizer#supportsKeepAlive()}.  Set to zero to disable this feature.
     */
    public GremlinClusterBuilder keepAliveInterval(final long keepAliveInterval) {
        this.keepAliveInterval = keepAliveInterval;
        return this;
    }

    /**
     * The X.509 certificate chain file in PEM format.
     *
     * @deprecated As of release 3.2.10, replaced by {@link #keyStore}
     */
    @Deprecated
    public GremlinClusterBuilder keyCertChainFile(final String keyCertChainFile) {
        this.keyCertChainFile = keyCertChainFile;
        return this;
    }

    /**
     * The PKCS#8 private key file in PEM format.
     *
     * @deprecated As of release 3.2.10, replaced by {@link #keyStore}
     */
    @Deprecated
    public GremlinClusterBuilder keyFile(final String keyFile) {
        this.keyFile = keyFile;
        return this;
    }

    /**
     * The password of the {@link #keyFile}, or {@code null} if it's not password-protected.
     *
     * @deprecated As of release 3.2.10, replaced by {@link #keyStorePassword}
     */
    @Deprecated
    public GremlinClusterBuilder keyPassword(final String keyPassword) {
        this.keyPassword = keyPassword;
        return this;
    }

    /**
     * The file location of the private key in JKS or PKCS#12 format.
     */
    public GremlinClusterBuilder keyStore(final String keyStore) {
        this.keyStore = keyStore;
        return this;
    }

    /**
     * The password of the {@link #keyStore}, or {@code null} if it's not password-protected.
     */
    public GremlinClusterBuilder keyStorePassword(final String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    /**
     * The file location for a SSL Certificate Chain to use when SSL is enabled. If
     * this value is not provided and SSL is enabled, the default {@link TrustManager} will be used.
     */
    public GremlinClusterBuilder trustStore(final String trustStore) {
        this.trustStore = trustStore;
        return this;
    }

    /**
     * The password of the {@link #trustStore}, or {@code null} if it's not password-protected.
     */
    public GremlinClusterBuilder trustStorePassword(final String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    /**
     * The format of the {@link #keyStore}, either {@code JKS} or {@code PKCS12}
     */
    public GremlinClusterBuilder keyStoreType(final String keyStoreType) {
        this.keyStoreType = keyStoreType;
        return this;
    }

    /**
     * A list of SSL protocols to enable. @see <a href=
     * "https://docs.oracle.com/javase/8/docs/technotes/guides/security/SunProviders.html#SunJSSE_Protocols">JSSE
     * Protocols</a>
     */
    public GremlinClusterBuilder sslEnabledProtocols(final List<String> sslEnabledProtocols) {
        this.sslEnabledProtocols = sslEnabledProtocols;
        return this;
    }

    /**
     * A list of cipher suites to enable. @see <a href=
     * "https://docs.oracle.com/javase/8/docs/technotes/guides/security/SunProviders.html#SupportedCipherSuites">Cipher
     * Suites</a>
     */
    public GremlinClusterBuilder sslCipherSuites(final List<String> sslCipherSuites) {
        this.sslCipherSuites = sslCipherSuites;
        return this;
    }

    /**
     * If true, trust all certificates and do not perform any validation.
     */
    public GremlinClusterBuilder sslSkipCertValidation(final boolean sslSkipCertValidation) {
        this.sslSkipCertValidation = sslSkipCertValidation;
        return this;
    }

    /**
     * The minimum number of in-flight requests that can occur on a {@link Connection} before it is considered
     * for closing on return to the {@link ConnectionPool}.
     */
    public GremlinClusterBuilder minInProcessPerConnection(final int minInProcessPerConnection) {
        this.minInProcessPerConnection = minInProcessPerConnection;
        return this;
    }

    /**
     * The maximum number of in-flight requests that can occur on a {@link Connection}. This represents an
     * indication of how busy a {@link Connection} is allowed to be.  This number is linked to the
     * {@link #maxSimultaneousUsagePerConnection} setting, but is slightly different in that it refers to
     * the total number of requests on a {@link Connection}.  In other words, a {@link Connection} might
     * be borrowed once to have multiple requests executed against it.  This number controls the maximum
     * number of requests whereas {@link #maxInProcessPerConnection} controls the times borrowed.
     */
    public GremlinClusterBuilder maxInProcessPerConnection(final int maxInProcessPerConnection) {
        this.maxInProcessPerConnection = maxInProcessPerConnection;
        return this;
    }

    /**
     * The maximum number of times that a {@link Connection} can be borrowed from the pool simultaneously.
     * This represents an indication of how busy a {@link Connection} is allowed to be.  Set too large and the
     * {@link Connection} may queue requests too quickly, rather than wait for an available {@link Connection}
     * or create a fresh one.  If set too small, the {@link Connection} will show as busy very quickly thus
     * forcing waits for available {@link Connection} instances in the pool when there is more capacity available.
     */
    public GremlinClusterBuilder maxSimultaneousUsagePerConnection(final int maxSimultaneousUsagePerConnection) {
        this.maxSimultaneousUsagePerConnection = maxSimultaneousUsagePerConnection;
        return this;
    }

    /**
     * The minimum number of times that a {@link Connection} should be borrowed from the pool before it falls
     * under consideration for closing.  If a {@link Connection} is not busy and the
     * {@link #minConnectionPoolSize} is exceeded, then there is no reason to keep that connection open.  Set
     * too large and {@link Connection} that isn't busy will continue to consume resources when it is not being
     * used.  Set too small and {@link Connection} instances will be destroyed when the driver might still be
     * busy.
     */
    public GremlinClusterBuilder minSimultaneousUsagePerConnection(final int minSimultaneousUsagePerConnection) {
        this.minSimultaneousUsagePerConnection = minSimultaneousUsagePerConnection;
        return this;
    }

    /**
     * The maximum size that the {@link ConnectionPool} can grow.
     */
    public GremlinClusterBuilder maxConnectionPoolSize(final int maxSize) {
        this.maxConnectionPoolSize = maxSize;
        return this;
    }

    /**
     * The minimum size of the {@link ConnectionPool}.  When the {@link Client} is started, {@link Connection}
     * objects will be initially constructed to this size.
     */
    public GremlinClusterBuilder minConnectionPoolSize(final int minSize) {
        this.minConnectionPoolSize = minSize;
        return this;
    }

    /**
     * Override the server setting that determines how many results are returned per batch.
     */
    public GremlinClusterBuilder resultIterationBatchSize(final int size) {
        this.resultIterationBatchSize = size;
        return this;
    }

    /**
     * The maximum amount of time to wait for a connection to be borrowed from the connection pool.
     */
    public GremlinClusterBuilder maxWaitForConnection(final int maxWait) {
        this.maxWaitForConnection = maxWait;
        return this;
    }

    /**
     * If the connection is using a "session" this setting represents the amount of time in milliseconds to wait
     * for that session to close before timing out where the default value is 3000. Note that the server will
     * eventually clean up dead sessions itself on expiration of the session or during shutdown.
     */
    public GremlinClusterBuilder maxWaitForSessionClose(final int maxWait) {
        this.maxWaitForSessionClose = maxWait;
        return this;
    }

    /**
     * The maximum size in bytes of any request sent to the server.   This number should not exceed the same
     * setting defined on the server.
     */
    public GremlinClusterBuilder maxContentLength(final int maxContentLength) {
        this.maxContentLength = maxContentLength;
        return this;
    }

    /**
     * Specify the {@link Channelizer} implementation to use on the client when creating a {@link Connection}.
     */
    public GremlinClusterBuilder channelizer(final String channelizerClass) {
        this.channelizer = channelizerClass;
        return this;
    }

    /**
     * Specify the {@link Channelizer} implementation to use on the client when creating a {@link Connection}.
     */
    public GremlinClusterBuilder channelizer(final Class channelizerClass) {
        return channelizer(channelizerClass.getCanonicalName());
    }

    /**
     * Specify a valid Gremlin script that can be used to test remote operations. This script should be designed
     * to return quickly with the least amount of overhead possible. By default, the script sends an empty string.
     * If the graph does not support that sort of script because it requires all scripts to include a reference
     * to a graph then a good option might be {@code g.inject()}.
     */
    public GremlinClusterBuilder validationRequest(final String script) {
        validationRequest = script;
        return this;
    }

    /**
     * Time in milliseconds to wait between retries when attempting to reconnect to a dead host.
     */
    public GremlinClusterBuilder reconnectInterval(final int interval) {
        this.reconnectInterval = interval;
        return this;
    }

    /**
     * Specifies the load balancing strategy to use on the client side.
     */
    public GremlinClusterBuilder loadBalancingStrategy(final Supplier<LoadBalancingStrategy> loadBalancingStrategy) {
        this.loadBalancingStrategy = loadBalancingStrategy;
        return this;
    }

    /**
     * Specifies parameters for authentication to Gremlin Server.
     */
    public GremlinClusterBuilder authProperties(final AuthProperties authProps) {
        this.authProps = authProps;
        return this;
    }

    /**
     * Sets the {@link AuthProperties.Property#USERNAME} and {@link AuthProperties.Property#PASSWORD} properties
     * for authentication to Gremlin Server.
     */
    public GremlinClusterBuilder credentials(final String username, final String password) {
        authProps = authProps.with(AuthProperties.Property.USERNAME, username).with(AuthProperties.Property.PASSWORD, password);
        return this;
    }

    /**
     * Sets the {@link AuthProperties.Property#PROTOCOL} properties for authentication to Gremlin Server.
     */
    public GremlinClusterBuilder protocol(final String protocol) {
        this.authProps = authProps.with(AuthProperties.Property.PROTOCOL, protocol);
        return this;
    }

    /**
     * Sets the {@link AuthProperties.Property#JAAS_ENTRY} properties for authentication to Gremlin Server.
     */
    public GremlinClusterBuilder jaasEntry(final String jaasEntry) {
        this.authProps = authProps.with(AuthProperties.Property.JAAS_ENTRY, jaasEntry);
        return this;
    }

    /**
     * Adds the address of a Gremlin Server to the list of servers a {@link Client} will try to contact to send
     * requests to.  The address should be parseable by {@link InetAddress#getByName(String)}.  That's the only
     * validation performed at this point.  No connection to the host is attempted.
     */
    public GremlinClusterBuilder addContactPoint(final String address) {
        try {
            InetAddress.getByName(address);
            this.addresses.add(address);
            return this;
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    /**
     * Add one or more the addresses of a Gremlin Servers to the list of servers a {@link Client} will try to
     * contact to send requests to.  The address should be parseable by {@link InetAddress#getByName(String)}.
     * That's the only validation performed at this point.  No connection to the host is attempted.
     */
    public GremlinClusterBuilder addContactPoints(final String... addresses) {
        for (String address : addresses)
            addContactPoint(address);
        return this;
    }

    public GremlinClusterBuilder addContactPoints(final Collection<String> addresses) {
        for (String address : addresses)
            addContactPoint(address);
        return this;
    }

    /**
     * Sets the port that the Gremlin Servers will be listening on.
     */
    public GremlinClusterBuilder port(final int port) {
        this.port = port;
        return this;
    }

    List<InetSocketAddress> getContactPoints() {
        return addresses.stream().map(addy -> new InetSocketAddress(addy, port)).collect(Collectors.toList());
    }

    public GremlinCluster create() {
        return new GremlinCluster(addresses, s -> {
            Cluster.Builder builder = Cluster.build()
                    .reconnectInterval(reconnectInterval)
                    .maxWaitForConnection(maxWaitForConnection)
                    .enableSsl(enableSsl)
                    .maxInProcessPerConnection(maxInProcessPerConnection)
                    .minSimultaneousUsagePerConnection(minSimultaneousUsagePerConnection)
                    .port(port)
                    .authProperties(authProps)
                    .loadBalancingStrategy(loadBalancingStrategy.get())
                    .validationRequest(validationRequest)
                    .channelizer(channelizer)
                    .maxContentLength(maxContentLength)
                    .maxWaitForSessionClose(maxWaitForSessionClose)
                    .resultIterationBatchSize(resultIterationBatchSize)
                    .minConnectionPoolSize(minConnectionPoolSize)
                    .maxConnectionPoolSize(maxConnectionPoolSize)
                    .maxSimultaneousUsagePerConnection(maxSimultaneousUsagePerConnection)
                    .minInProcessPerConnection(minInProcessPerConnection)
                    .sslSkipCertValidation(sslSkipCertValidation)
                    .sslCipherSuites(sslCipherSuites)
                    .sslEnabledProtocols(sslEnabledProtocols)
                    .keyStoreType(keyStoreType)
                    .trustStorePassword(trustStorePassword)
                    .trustStore(trustStore)
                    .keyStorePassword(keyStorePassword)
                    .keyStore(keyStore)
                    .keepAliveInterval(keepAliveInterval)
                    .sslContext(sslContext)
                    .serializer(serializer)
                    .path(path)
                    .workerPoolSize(workerPoolSize)
                    .nioPoolSize(nioPoolSize);
            if (s != null) {
                builder = builder.addContactPoint(s);
            }
            return builder.create();
        }, refreshOnErrorThreshold, refreshOnErrorEventHandler);
    }
}
