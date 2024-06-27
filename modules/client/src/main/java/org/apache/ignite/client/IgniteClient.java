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

import static org.apache.ignite.client.IgniteClientConfiguration.DFLT_CONNECT_TIMEOUT;
import static org.apache.ignite.client.IgniteClientConfiguration.DFLT_HEARTBEAT_INTERVAL;
import static org.apache.ignite.client.IgniteClientConfiguration.DFLT_HEARTBEAT_TIMEOUT;
import static org.apache.ignite.client.IgniteClientConfiguration.DFLT_OPERATION_TIMEOUT;
import static org.apache.ignite.client.IgniteClientConfiguration.DFLT_RECONNECT_INTERVAL;
import static org.apache.ignite.client.IgniteClientConfiguration.DFLT_RECONNECT_THROTTLING_PERIOD;
import static org.apache.ignite.client.IgniteClientConfiguration.DFLT_RECONNECT_THROTTLING_RETRIES;
import static org.apache.ignite.internal.client.ClientUtils.sync;

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

    /** Client builder. */
    @SuppressWarnings("PublicInnerClass")
    class Builder {
        /** Addresses. */
        private String[] addresses;

        /** Address finder. */
        private IgniteClientAddressFinder addressFinder;

        /** Connect timeout. */
        private long connectTimeout = DFLT_CONNECT_TIMEOUT;

        /** Reconnect throttling period. */
        private long reconnectThrottlingPeriod = DFLT_RECONNECT_THROTTLING_PERIOD;

        /** Reconnect throttling retries. */
        private int reconnectThrottlingRetries = DFLT_RECONNECT_THROTTLING_RETRIES;

        /** Reconnect interval, in milliseconds. */
        private long reconnectInterval = DFLT_RECONNECT_INTERVAL;

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
         * Sets the reconnect throttling period, in milliseconds.
         *
         * <p>Default is {@link IgniteClientConfiguration#DFLT_RECONNECT_THROTTLING_PERIOD}.
         *
         * @param reconnectThrottlingPeriod Reconnect throttling period, in milliseconds.
         * @return This instance.
         */
        public Builder reconnectThrottlingPeriod(long reconnectThrottlingPeriod) {
            this.reconnectThrottlingPeriod = reconnectThrottlingPeriod;

            return this;
        }

        /**
         * Sets the reconnect throttling retries.
         *
         * <p>Default is {@link IgniteClientConfiguration#DFLT_RECONNECT_THROTTLING_RETRIES}.
         *
         * @param reconnectThrottlingRetries Reconnect throttling retries.
         * @return This instance.
         * @throws IllegalArgumentException When value is less than zero.
         */
        public Builder reconnectThrottlingRetries(int reconnectThrottlingRetries) {
            if (reconnectThrottlingRetries < 0) {
                throw new IllegalArgumentException("Reconnect throttling retries ["
                        + reconnectThrottlingRetries + "] must be a non-negative integer value.");
            }

            this.reconnectThrottlingRetries = reconnectThrottlingRetries;

            return this;
        }

        /**
         * Sets the reconnect interval, in milliseconds. Set to {@code 0} to disable background reconnect.
         *
         * <p>Ignite balances requests across all healthy connections (when multiple endpoints are configured).
         * Ignite also repairs connections on demand (when a request is made).
         * However, "secondary" connections can be lost (due to network issues, or node restarts). This property controls how ofter Ignite
         * client will check all configured endpoints and try to reconnect them in case of failure.
         *
         * @param reconnectInterval Reconnect interval, in milliseconds.
         * @return This instance.
         * @throws IllegalArgumentException When value is less than zero.
         */
        public Builder reconnectInterval(long reconnectInterval) {
            if (reconnectInterval < 0) {
                throw new IllegalArgumentException("reconnectInterval ["
                        + reconnectInterval + "] must be a non-negative integer value.");
            }

            this.reconnectInterval = reconnectInterval;

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
                    reconnectThrottlingPeriod,
                    reconnectThrottlingRetries,
                    reconnectInterval,
                    asyncContinuationExecutor,
                    heartbeatInterval,
                    heartbeatTimeout,
                    retryPolicy,
                    loggerFactory,
                    sslConfiguration,
                    metricsEnabled,
                    authenticator,
                    operationTimeout);

            return TcpIgniteClient.startAsync(cfg);
        }
    }
}
