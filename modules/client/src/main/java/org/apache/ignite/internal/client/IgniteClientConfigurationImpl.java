/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.ignite.client.IgniteClientAddressFinder;
import org.apache.ignite.client.IgniteClientConfiguration;

/**
 * Immutable configuration.
 */
public class IgniteClientConfigurationImpl implements IgniteClientConfiguration {
    /** Address finder. */
    private final IgniteClientAddressFinder addressFinder;

    /** Addresses. */
    private final String[] addresses;

    /** Retry limit. */
    private final int retryLimit;

    /** Connect timeout. */
    private final int connectTimeout;

    /** Reconnect throttling period.  */
    private final long reconnectThrottlingPeriod;

    /** Reconnect throttling retries. */
    private final int reconnectThrottlingRetries;

    /**
     * Constructor.
     *  @param addressFinder Address finder.
     * @param addresses Addresses.
     * @param retryLimit Retry limit.
     * @param connectTimeout Socket connect timeout.
     */
    public IgniteClientConfigurationImpl(
            IgniteClientAddressFinder addressFinder,
            String[] addresses,
            int retryLimit,
            int connectTimeout,
            int reconnectThrottlingPeriod,
            int reconnectThrottlingRetries
    ) {
        this.addressFinder = addressFinder;
        this.addresses = addresses;
        this.retryLimit = retryLimit;
        this.connectTimeout = connectTimeout;
        this.reconnectThrottlingPeriod = reconnectThrottlingPeriod;
        this.reconnectThrottlingRetries = reconnectThrottlingRetries;
    }

    /** {@inheritDoc} */
    @Override public IgniteClientAddressFinder getAddressesFinder() {
        return addressFinder;
    }

    /** {@inheritDoc} */
    @Override public String[] getAddresses() {
        // TODO: Defensive copy IGNITE-15164.
        return addresses;
    }

    /** {@inheritDoc} */
    @Override public int getRetryLimit() {
        return retryLimit;
    }

    /** {@inheritDoc} */
    @Override public int getConnectTimeout() {
        return connectTimeout;
    }

    /** {@inheritDoc} */
    @Override public long getReconnectThrottlingPeriod() {
        return reconnectThrottlingPeriod;
    }

    /** {@inheritDoc} */
    @Override public int getReconnectThrottlingRetries() {
        return reconnectThrottlingRetries;
    }
}