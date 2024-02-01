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

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import org.apache.ignite.lang.LoggerFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite client configuration.
 */
public interface IgniteClientConfiguration {
    /** Default port. */
    int DFLT_PORT = 10800;

    /** Default socket connect timeout, in milliseconds. */
    int DFLT_CONNECT_TIMEOUT = 5000;

    /** Default heartbeat timeout, in milliseconds. */
    int DFLT_HEARTBEAT_TIMEOUT = 5000;

    /** Default heartbeat interval, in milliseconds. */
    int DFLT_HEARTBEAT_INTERVAL = 30_000;

    /** Default reconnect throttling period, in milliseconds. */
    long DFLT_RECONNECT_THROTTLING_PERIOD = 30_000L;

    /** Default reconnect throttling retries. */
    int DFLT_RECONNECT_THROTTLING_RETRIES = 3;

    /** Default reconnect interval, in milliseconds. */
    long DFLT_RECONNECT_INTERVAL = 30_000L;

    /** Default operation timeout, in milliseconds. */
    int DFLT_OPERATION_TIMEOUT = 0;

    /**
     * Gets the address finder.
     *
     * @return Address finder.
     */
    IgniteClientAddressFinder addressesFinder();

    /**
     * Gets the addresses of Ignite server nodes within a cluster. An address can be an IP address or a hostname, with or without port. If
     * port is not set then Ignite will use {@link IgniteClientConfiguration#DFLT_PORT}.
     *
     * <p>Providing addresses of multiple nodes in the cluster will improve performance: Ignite will balance requests across all
     * connections, and use partition awareness to send key-based requests directly to the primary node.
     *
     * @return Addresses.
     */
    String[] addresses();

    /**
     * Gets the retry policy. When a request fails due to a connection error, and multiple server connections
     * are available, Ignite will retry the request if the specified policy allows it.
     *
     * @return Retry policy.
     */
    @Nullable RetryPolicy retryPolicy();

    /**
     * Gets the socket connect timeout, in milliseconds.
     *
     * @return Socket connect timeout, in milliseconds.
     */
    long connectTimeout();

    /**
     * Gets the reconnect throttling period, in milliseconds.
     *
     * @return Reconnect period for throttling, in milliseconds.
     */
    long reconnectThrottlingPeriod();

    /**
     * Gets the reconnect throttling retries.
     *
     * @return Reconnect throttling retries.
     */
    int reconnectThrottlingRetries();

    /**
     * Gets the background reconnect interval, in milliseconds. Set to {@code 0} to disable background reconnect.
     * Default is {@link #DFLT_RECONNECT_INTERVAL}.
     *
     * <p>Ignite balances requests across all healthy connections (when multiple endpoints are configured).
     * Ignite also repairs connections on demand (when a request is made).
     * However, "secondary" connections can be lost (due to network issues, or node restarts). This property controls how ofter Ignite
     * client will check all configured endpoints and try to reconnect them in case of failure.
     *
     * @return Background reconnect interval, in milliseconds.
     */
    long reconnectInterval();

    /**
     * Gets the async continuation executor.
     *
     * <p>When {@code null} (default), {@link ForkJoinPool#commonPool()} is used.
     *
     * <p>When async client operation completes, corresponding {@link java.util.concurrent.CompletableFuture} continuations
     * (such as {@link java.util.concurrent.CompletableFuture#thenApply(Function)}) will be invoked using this executor.
     *
     * <p>Server responses are handled by a dedicated network thread. To ensure optimal performance,
     * this thread should not perform any extra work, so user-defined continuations are offloaded to the specified executor.
     *
     * @return Executor for async continuations.
     */
    @Nullable Executor asyncContinuationExecutor();

    /**
     * Gets the heartbeat message interval, in milliseconds. Default is {@code 30_000}.
     *
     * <p>When server-side idle timeout is not zero, effective heartbeat
     * interval is set to {@code min(heartbeatInterval, idleTimeout / 3)}.
     *
     * <p>When thin client connection is idle (no operations are performed), heartbeat messages are sent periodically
     * to keep the connection alive and detect potential half-open state.
     *
     * @return Heartbeat interval.
     */
    long heartbeatInterval();

    /**
     * Gets the heartbeat message timeout, in milliseconds. Default is {@code 5000}.
     *
     * <p>When a server does not respond to a heartbeat within the specified timeout, client will close the connection.
     *
     * <p>When thin client connection is idle (no operations are performed), heartbeat messages are sent periodically
     * to keep the connection alive and detect potential half-open state.
     *
     * @return Heartbeat interval.
     */
    long heartbeatTimeout();

    /**
     * Returns the logger factory. This factory will be used to create a logger instance when needed.
     *
     * <p>When {@code null} (default), {@link System#getLogger} is used.
     *
     * @return Configured logger factory.
     */
    @Nullable LoggerFactory loggerFactory();

    /**
     * Returns the client SSL configuration. This configuration will be used to setup the SSL connection with
     * the Ignite 3 nodes.
     *
     * <p>When {@code null} then no SSL is used.
     *
     * @return Client SSL configuration.
     */
    @Nullable SslConfiguration ssl();

    /**
     * Gets a value indicating whether JMX metrics are enabled.
     *
     * @return {@code true} if metrics are enabled.
     */
    boolean metricsEnabled();

    /**
     * Gets the authenticator.
     *
     * <p>See also: {@link BasicAuthenticator}.
     *
     * @return Authenticator.
     */
    @Nullable IgniteClientAuthenticator authenticator();

    /**
     * Gets the operation timeout, in milliseconds. Default is {@code 0} (no timeout).
     *
     * @return Operation timeout, in milliseconds.
     */
    long operationTimeout();
}
