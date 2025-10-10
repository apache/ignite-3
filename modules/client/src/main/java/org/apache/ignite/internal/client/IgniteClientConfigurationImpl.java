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

package org.apache.ignite.internal.client;

import java.util.concurrent.Executor;
import org.apache.ignite.client.IgniteClientAddressFinder;
import org.apache.ignite.client.IgniteClientAuthenticator;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.client.RetryPolicy;
import org.apache.ignite.client.SslConfiguration;
import org.apache.ignite.lang.LoggerFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Immutable client configuration.
 */
public final class IgniteClientConfigurationImpl implements IgniteClientConfiguration {
    /** Address finder. */
    private final @Nullable IgniteClientAddressFinder addressFinder;

    /** Addresses. */
    private final String[] addresses;

    /** Connect timeout, in milliseconds. */
    private final long connectTimeout;

    /** Background reconnect interval, in milliseconds. */
    private final long backgroundReconnectInterval;

    /** Async continuation executor. */
    private final @Nullable Executor asyncContinuationExecutor;

    /** Heartbeat interval. */
    private final long heartbeatInterval;

    /** Heartbeat timout. */
    private final long heartbeatTimeout;

    /** Retry policy. */
    private final RetryPolicy retryPolicy;

    private final LoggerFactory loggerFactory;

    private final SslConfiguration sslConfiguration;

    private final boolean metricsEnabled;

    private final @Nullable  IgniteClientAuthenticator authenticator;

    private final long operationTimeout;

    private final int sqlPartitionAwarenessMetadataCacheSize;

    private final @Nullable String name;

    InetAddressResolver addressResolver = InetAddressResolver.DEFAULT;

    /**
     * Constructor.
     *
     * @param addressFinder Address finder.
     * @param addresses Addresses.
     * @param connectTimeout Socket connect timeout.
     * @param backgroundReconnectInterval Background reconnect interval.
     * @param asyncContinuationExecutor Async continuation executor.
     * @param heartbeatInterval Heartbeat message interval.
     * @param heartbeatTimeout Heartbeat message timeout.
     * @param retryPolicy Retry policy.
     * @param loggerFactory Logger factory which will be used to create a logger instance for this this particular client when
     *         needed.
     * @param metricsEnabled Whether metrics are enabled.
     * @param authenticator Authenticator.
     * @param operationTimeout Operation timeout.
     * @param sqlPartitionAwarenessMetadataCacheSize Size of the cache to store partition awareness metadata.
     * @param name Client name.
     */
    public IgniteClientConfigurationImpl(
            @Nullable IgniteClientAddressFinder addressFinder,
            String[] addresses,
            long connectTimeout,
            long backgroundReconnectInterval,
            @Nullable Executor asyncContinuationExecutor,
            long heartbeatInterval,
            long heartbeatTimeout,
            @Nullable RetryPolicy retryPolicy,
            @Nullable LoggerFactory loggerFactory,
            @Nullable SslConfiguration sslConfiguration,
            boolean metricsEnabled,
            @Nullable IgniteClientAuthenticator authenticator,
            long operationTimeout,
            int sqlPartitionAwarenessMetadataCacheSize,
            @Nullable String name
    ) {
        this.addressFinder = addressFinder;

        //noinspection AssignmentOrReturnOfFieldWithMutableType (cloned in Builder).
        this.addresses = addresses;

        this.connectTimeout = connectTimeout;
        this.backgroundReconnectInterval = backgroundReconnectInterval;
        this.asyncContinuationExecutor = asyncContinuationExecutor;
        this.heartbeatInterval = heartbeatInterval;
        this.heartbeatTimeout = heartbeatTimeout;
        this.retryPolicy = retryPolicy;
        this.loggerFactory = loggerFactory;
        this.sslConfiguration = sslConfiguration;
        this.metricsEnabled = metricsEnabled;
        this.authenticator = authenticator;
        this.operationTimeout = operationTimeout;
        this.sqlPartitionAwarenessMetadataCacheSize = sqlPartitionAwarenessMetadataCacheSize;
        this.name = name;
        this.addressResolver = addressResolver == null ? InetAddressResolver.DEFAULT : addressResolver;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable IgniteClientAddressFinder addressesFinder() {
        return addressFinder;
    }

    /** {@inheritDoc} */
    @Override
    public String[] addresses() {
        return addresses == null ? null : addresses.clone();
    }

    /** {@inheritDoc} */
    @Override
    public long connectTimeout() {
        return connectTimeout;
    }

    /** {@inheritDoc} */
    @Override
    public long backgroundReconnectInterval() {
        return backgroundReconnectInterval;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Executor asyncContinuationExecutor() {
        return asyncContinuationExecutor;
    }

    /** {@inheritDoc} */
    @Override
    public long heartbeatInterval() {
        return heartbeatInterval;
    }

    /** {@inheritDoc} */
    @Override
    public long heartbeatTimeout() {
        return heartbeatTimeout;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable LoggerFactory loggerFactory() {
        return loggerFactory;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable RetryPolicy retryPolicy() {
        return retryPolicy;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable SslConfiguration ssl() {
        return sslConfiguration;
    }

    /** {@inheritDoc} */
    @Override
    public boolean metricsEnabled() {
        return metricsEnabled;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteClientAuthenticator authenticator() {
        return authenticator;
    }

    @Override
    public long operationTimeout() {
        return operationTimeout;
    }

    @Override
    public int sqlPartitionAwarenessMetadataCacheSize() {
        return sqlPartitionAwarenessMetadataCacheSize;
    }

    @Override
    public @Nullable String name() {
        return name;
    }
}
