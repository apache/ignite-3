/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.client;

import static org.apache.ignite.client.IgniteClientConfiguration.DFLT_BACKGROUND_RECONNECT_INTERVAL;
import static org.apache.ignite.client.IgniteClientConfiguration.DFLT_BACKGROUND_RE_RESOLVE_ADDRESSES_INTERVAL;
import static org.apache.ignite.client.IgniteClientConfiguration.DFLT_CONNECT_TIMEOUT;
import static org.apache.ignite.client.IgniteClientConfiguration.DFLT_HEARTBEAT_INTERVAL;
import static org.apache.ignite.client.IgniteClientConfiguration.DFLT_HEARTBEAT_TIMEOUT;
import static org.apache.ignite.client.IgniteClientConfiguration.DFLT_OPERATION_TIMEOUT;
import static org.apache.ignite.client.IgniteClientConfiguration.DFLT_SQL_PARTITION_AWARENESS_METADATA_CACHE_SIZE;
import static org.apache.ignite.internal.util.ViewUtils.sync;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.client.IgniteClientConfigurationImpl;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.lang.LoggerFactory;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.sql.IgniteSql;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite client entry point.
 */
public interface IgniteClient extends Ignite, AutoCloseable {
    /**
     * Gets the configuration.
     *
     * @return Configuration.
     */
    IgniteClientConfiguration configuration();

    /**
     * Gets active client connections.
     *
     * @return List of connected cluster nodes.
     */
    List<ClusterNode> connections();

    /**
     * Gets a new client builder.
     *
     * @return New client builder.
     */
    static Builder builder() {
        return new Builder();
    }

    @Override
    void close();

    /** Client builder. */
    @SuppressWarnings("PublicInnerClass")
    class Builder {
        /** Addresses. */
        private String[] addresses;

        /** Address finder. */
        private IgniteClientAddressFinder addressFinder;

        /** Connect timeout. */
        private long connectTimeout = DFLT_CONNECT_TIMEOUT;

        /** Reconnect interval, in milliseconds. */
        private long backgroundReconnectInterval = DFLT_BACKGROUND_RECONNECT_INTERVAL;

        /** Async continuation executor. */
        private Executor asyncContinuationExecutor;

        /** Heartbeat interval. */
        private long heartbeatInterval = DFLT_HEARTBEAT_INTERVAL;

        /** Heartbeat timeout. */
        private long heartbeatTimeout = DFLT_HEARTBEAT_TIMEOUT;

        /** Retry policy. */
        private @Nullable RetryPolicy retryPolicy = new RetryReadPolicy();

        /** Logger factory. */
        private @Nullable LoggerFactory loggerFactory;

        /** SSL configuration. */
        private @Nullable SslConfiguration sslConfiguration;

        /** Metrics enabled flag. */
        private boolean metricsEnabled;

        /** Authenticator. */
        private @Nullable IgniteClientAuthenticator authenticator;

        /** Operation timeout. */
        private long operationTimeout = DFLT_OPERATION_TIMEOUT;

        private int sqlPartitionAwarenessMetadataCacheSize = DFLT_SQL_PARTITION_AWARENESS_METADATA_CACHE_SIZE;

        private @Nullable String name;

        long backgroundReResolveAddressesInterval = DFLT_BACKGROUND_RE_RESOLVE_ADDRESSES_INTERVAL;

        /**
         * Sets the addresses of Ignite server nodes within a cluster. An address can be an IP address or a hostname, with or without port.
         * If port is not set then Ignite will use the default one - see {@link IgniteClientConfiguration#DFLT_PORT}.
         *
         * @param addrs Addresses.
         * @return This instance.
         */
        public Builder addresses(String... addrs) {
            Objects.requireNonNull(addrs, "addrs is null");

            addresses = addrs.clone();

            return this;
        }

        /**
         * Sets the retry policy. When a request fails due to a connection error, and multiple server connections
         * are available, Ignite will retry the request if the specified policy allows it.
         *
         * <p>Default is {@link RetryReadPolicy}.
         *
         * @param retryPolicy Retry policy.
         * @return This instance.
         */
        public Builder retryPolicy(@Nullable RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;

            return this;
        }

        /**
         * Sets the logger factory. This factory will be used to create a logger instance when needed.
         *
         * <p>When {@code null} (default), {@link System#getLogger} is used.
         *
         * @param loggerFactory A factory.
         * @return This instance.
         */
        public Builder loggerFactory(@Nullable LoggerFactory loggerFactory) {
            this.loggerFactory = loggerFactory;

            return this;
        }

        /**
         * Sets the socket connection timeout, in milliseconds.
         *
         * <p>Default is {@link IgniteClientConfiguration#DFLT_CONNECT_TIMEOUT}.
         *
         * @param connectTimeout Socket connection timeout, in milliseconds.
         * @return This instance.
         * @throws IllegalArgumentException When value is less than zero.
         */
        public Builder connectTimeout(long connectTimeout) {
            if (connectTimeout < 0) {
                throw new IllegalArgumentException("Connect timeout [" + connectTimeout + "] "
                        + "must be a non-negative integer value.");
            }

            this.connectTimeout = connectTimeout;

            return this;
        }

        /**
         * Sets the address finder.
         *
         * @param addressFinder Address finder.
         * @return This instance.
         */
        public Builder addressFinder(IgniteClientAddressFinder addressFinder) {
            this.addressFinder = addressFinder;

            return this;
        }

        /**
         * Sets the background reconnect interval, in milliseconds. Set to {@code 0} to disable background reconnect.
         *
         * <p>Ignite balances requests across all healthy connections (when multiple endpoints are configured).
         * Ignite also repairs connections on demand (when a request is made).
         * However, "secondary" connections can be lost (due to network issues, or node restarts). This property controls how ofter Ignite
         * client will check all configured endpoints and try to reconnect them in case of failure.
         *
         * @param backgroundReconnectInterval Reconnect interval, in milliseconds.
         * @return This instance.
         * @throws IllegalArgumentException When value is less than zero.
         */
        public Builder backgroundReconnectInterval(long backgroundReconnectInterval) {
            if (backgroundReconnectInterval < 0) {
                throw new IllegalArgumentException("backgroundReconnectInterval ["
                        + backgroundReconnectInterval + "] must be a non-negative integer value.");
            }

            this.backgroundReconnectInterval = backgroundReconnectInterval;

            return this;
        }

        /**
         * Sets the async continuation executor.
         *
         * <p>When <code>null</code> (default), {@link ForkJoinPool#commonPool()} is used.
         *
         * <p>When async client operation completes, corresponding {@link java.util.concurrent.CompletableFuture} continuations
         * (such as {@link java.util.concurrent.CompletableFuture#thenApply(Function)}) will be invoked using this executor.
         *
         * <p>Server responses are handled by a dedicated network thread. To ensure optimal performance,
         * this thread should not perform any extra work, so user-defined continuations are offloaded to the specified executor.
         *
         * @param asyncContinuationExecutor Async continuation executor.
         * @return This instance.
         */
        public Builder asyncContinuationExecutor(Executor asyncContinuationExecutor) {
            this.asyncContinuationExecutor = asyncContinuationExecutor;

            return this;
        }

        /**
         * Sets the heartbeat message interval, in milliseconds. Default is <code>30_000</code>.
         *
         * <p>When server-side idle timeout is not zero, effective heartbeat
         * interval is set to <code>min(heartbeatInterval, idleTimeout / 3)</code>.
         *
         * <p>When thin client connection is idle (no operations are performed), heartbeat messages are sent periodically
         * to keep the connection alive and detect potential half-open state.
         *
         * @param heartbeatInterval Heartbeat interval.
         * @return This instance.
         */
        public Builder heartbeatInterval(long heartbeatInterval) {
            if (heartbeatInterval < 0) {
                throw new IllegalArgumentException("Heartbeat interval [" + heartbeatInterval + "] "
                        + "must be a non-negative integer value.");
            }

            this.heartbeatInterval = heartbeatInterval;

            return this;
        }

        /**
         * Sets the heartbeat message timeout, in milliseconds. Default is <code>5000</code>.
         *
         * <p>When a server does not respond to a heartbeat within the specified timeout, client will close the connection.
         *
         * @param heartbeatTimeout Heartbeat timeout.
         * @return This instance.
         */
        public Builder heartbeatTimeout(long heartbeatTimeout) {
            if (heartbeatTimeout < 0) {
                throw new IllegalArgumentException("Heartbeat timeout [" + heartbeatTimeout + "] "
                        + "must be a non-negative integer value.");
            }

            this.heartbeatTimeout = heartbeatTimeout;

            return this;
        }

        /**
         * Sets the SSL configuration.
         *
         * @param sslConfiguration SSL configuration.
         * @return This instance.
         */
        public Builder ssl(@Nullable SslConfiguration sslConfiguration) {
            this.sslConfiguration = sslConfiguration;

            return this;
        }

        /**
         * Enables or disables JMX metrics.
         *
         * <p>When enabled, Ignite client will expose JMX metrics via the platform MBean server
         * with bean name "org.apache.ignite:group=metrics,name=client".
         *
         * <p>Use {@code jconsole} or any other JMX client to view the metrics.
         *
         * <p>To get metrics programmatically:
         * {@code MBeanServerInvocationHandler.newProxyInstance(ManagementFactory.getPlatformMBeanServer(),
         * new ObjectName("org.apache.ignite:group=metrics,name=client"), DynamicMBean.class, false).getAttribute("ConnectionsActive")}
         *
         * <p>See <a href="https://docs.oracle.com/javase/8/docs/technotes/guides/jmx/">Java Management Extensions (JMX)</a>
         * for more information.
         *
         * @param metricsEnabled Metrics enabled flag.
         * @return This instance.
         */
        public Builder metricsEnabled(boolean metricsEnabled) {
            this.metricsEnabled = metricsEnabled;

            return this;
        }

        /**
         * Sets the authenticator.
         *
         * <p>See also: {@link BasicAuthenticator}.
         *
         * @param authenticator Authenticator.
         * @return This instance.
         */
        public Builder authenticator(@Nullable IgniteClientAuthenticator authenticator) {
            this.authenticator = authenticator;

            return this;
        }

        /**
         * Sets the operation timeout, in milliseconds. Default is {@code 0} (no timeout).
         *
         * <p>An "operation" is a single client request to the server. Some public API calls may involve multiple operations, in
         * which case the operation timeout is applied to each individual network call.
         *
         * @param operationTimeout Operation timeout, in milliseconds.
         * @return This instance.
         * @throws IllegalArgumentException When value is less than zero.
         */
        public Builder operationTimeout(long operationTimeout) {
            if (operationTimeout < 0) {
                throw new IllegalArgumentException("Operation timeout [" + operationTimeout + "] "
                        + "must be a non-negative integer value.");
            }

            this.operationTimeout = operationTimeout;

            return this;
        }

        /**
         * Sets the size of cache to store partition awareness metadata of sql queries, in number of entries. Default is
         * {@value IgniteClientConfiguration#DFLT_SQL_PARTITION_AWARENESS_METADATA_CACHE_SIZE}.
         *
         * <p>SQL partition awareness feature improves query performance by directing queries to the specific server nodes that hold the
         * relevant data, minimizing network overhead. Ignite client builds the metadata cache during the initial query execution and
         * leverages this cache to speed up subsequent queries.
         *
         * <p>Every instance of {@link IgniteSql} has its own cache. Every unique pair of (defaultSchema, queryString) reserve
         * its own place in metadata cache, if metadata is available for this particular query. In general, metadata is available for
         * queries which have equality predicate over all colocation columns, or which inserts the whole tuple. Let's consider the following
         * example:
         * <pre>
         *     // Creates reservations table. Please mind the {@code COLOCATE BY (floor_no)}: all reservations are colocated by
         *     // {@code floor_no}.
         *     CREATE TABLE RoomsReservations (room_no INT, floor_no INT, PRIMARY_KEY (room_no, floor_no)) COLOCATE BY (floor_no);
         *
         *     // Here, we are selecting all reserved rooms on a particular floor. All reservation are colocated by {@code floor_no},
         *     // therefore having predicate like {@code floor_no = ?} make it possible to compute a partition which keeps the data of
         *     // interest. Which in turn makes it possible to send the query directly to the node that hold the relevant data.
         *     SELECT room_no FROM RoomsReservations WHERE floor_no = ?;
         *
         *     // Similar with INSERT: since values of dynamic parameters are known on a client, it makes it possible to route the
         *     // query directly to the node that hold the relevant data.
         *     INSERT INTO RoomsReservations(room_no, floor_no) VALUES(?, ?);
         * </pre>
         *
         * @param size Cache size, in number of entries.
         * @return This instance.
         * @throws IllegalArgumentException When value is less than zero.
         */
        public Builder sqlPartitionAwarenessMetadataCacheSize(int size) {
            if (size < 0) {
                throw new IllegalArgumentException("Partition awareness metadata cache size [" + size + "] "
                        + "must be a non-negative integer value.");
            }

            this.sqlPartitionAwarenessMetadataCacheSize = size;

            return this;
        }

        /**
         * Sets the client name. Default is {@code null}, which means that Ignite will generate a unique name automatically.
         *
         * <p>Client name is used for identifying clients in JMX metrics. The name is only used locally and is not sent to the server.
         *
         * <p>If multiple clients with the same exist in the same JVM, JMX metrics will be exposed only for one of them.
         * Others will log an error.
         *
         * @param name Client name.
         * @return This instance.
         */
        public Builder name(@Nullable String name) {
            this.name = name;

            return this;
        }

        /**
         * Sets how long the resolved addresses will be considered valid, in milliseconds. Set to {@code 0} for infinite validity.
         *
         * <p>Ignite client resolve the provided hostnames into multiple IP addresses, each corresponds to an active cluster node.
         * However, additional IP addresses can be collected after updating the DNS records. This property controls how often Ignite
         * client will try to re-resolve provided hostnames and connect to newly discovered addresses.
         *
         * @param backgroundReResolveAddressesInterval  Background re-resolve interval, in milliseconds.
         * @return This instance.
         * @throws IllegalArgumentException When value is less than zero.
         */
        public Builder backgroundReResolveAddressesInterval(long backgroundReResolveAddressesInterval) {
            if (backgroundReResolveAddressesInterval < 0) {
                throw new IllegalArgumentException("backgroundReResolveAddressesInterval ["
                        + backgroundReResolveAddressesInterval + "] must be a non-negative integer value.");
            }

            this.backgroundReResolveAddressesInterval = backgroundReResolveAddressesInterval;

            return this;
        }

        /**
         * Builds the client.
         *
         * @return Ignite client.
         */
        public IgniteClient build() {
            return sync(buildAsync());
        }

        /**
         * Builds the client.
         *
         * @return Ignite client.
         */
        public CompletableFuture<IgniteClient> buildAsync() {
            var cfg = new IgniteClientConfigurationImpl(
                    addressFinder,
                    addresses,
                    connectTimeout,
                    backgroundReconnectInterval,
                    asyncContinuationExecutor,
                    heartbeatInterval,
                    heartbeatTimeout,
                    retryPolicy,
                    loggerFactory,
                    sslConfiguration,
                    metricsEnabled,
                    authenticator,
                    operationTimeout,
                    sqlPartitionAwarenessMetadataCacheSize,
                    name,
                    backgroundReResolveAddressesInterval
            );

            return TcpIgniteClient.startAsync(cfg);
        }
    }
}
