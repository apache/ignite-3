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

package org.apache.ignite.client;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.ignite.app.Ignite;
import org.apache.ignite.internal.client.IgniteClientConfigurationImpl;
import org.apache.ignite.internal.client.TcpIgniteClient;

/**
 * Ignite client entry point.
 */
public interface IgniteClient extends Ignite {
    /**
     * Gets the configuration.
     *
     * @return Configuration.
     */
    IgniteClientConfiguration configuration();

    /**
     * Gets a new client builder.
     *
     * @return New client builder.
     */
    static Builder builder() {
        return new Builder();
    }

    /** Client builder. */
    class Builder {
        /** Addresses. */
        private String[] addresses;

        /** Address finder. */
        private IgniteClientAddressFinder addressFinder;

        /** Retry limit. */
        private int retryLimit;

        /** Connect timeout. */
        private long connectTimeout;

        /** Reconnect throttling period. */
        private long reconnectThrottlingPeriod = 30_000L;

        /** Reconnect throttling retries. */
        private int reconnectThrottlingRetries = 3;

        /**
         * Sets the addresses of Ignite server nodes within a cluster. An address can be an IP address or a hostname,
         * with or without port. If port is not set then Ignite will generate multiple addresses for default port range.
         * See {@link IgniteClientConfiguration#DFLT_PORT}, {@link IgniteClientConfiguration#DFLT_PORT_RANGE}.
         *
         * @param addrs Addresses.
         */
        public Builder addresses(String... addrs) {
            Objects.requireNonNull(addrs, "addrs is null");

            addresses = addrs;

            return this;
        }

        /**
         * Sets the retry limit. 0 to disable retries.
         *
         * @param retryLimit Retry limit.
         * @return This instance.
         */
        public Builder retryLimit(int retryLimit) {
            if (retryLimit < 0)
                throw new IllegalArgumentException("Retry limit [" + retryLimit + "] " +
                        "must be a non-negative integer value.");

            this.retryLimit = retryLimit;

            return this;
        }

        /**
         * Sets the socket connection timeout, in milliseconds.
         *
         * @param connectTimeout Socket connection timeout, in milliseconds.
         * @return This instance.
         */
        public Builder connectTimeout(long connectTimeout) {
            if (connectTimeout < 0)
                throw new IllegalArgumentException("Connect timeout [" + connectTimeout + "] " +
                        "must be a non-negative integer value.");

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
         * @param reconnectThrottlingRetries Reconnect throttling retries.
         * @return This instance.
         */
        public Builder reconnectThrottlingRetries(int reconnectThrottlingRetries) {
            if (reconnectThrottlingRetries < 0)
                throw new IllegalArgumentException("Reconnect throttling retries ["
                        + reconnectThrottlingRetries + "] must be a non-negative integer value.");

            this.reconnectThrottlingRetries = reconnectThrottlingRetries;

            return this;
        }

        /**
         * Builds the client.
         *
         * @return Ignite client.
         */
        public IgniteClient build() {
            if (addresses == null)
                throw new IllegalArgumentException("Can't create Ignite client: addresses are not set.");

            return buildAsync().join();
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
                    retryLimit,
                    connectTimeout,
                    reconnectThrottlingPeriod,
                    reconnectThrottlingRetries);

            return TcpIgniteClient.startAsync(cfg);
        }
    }
}
