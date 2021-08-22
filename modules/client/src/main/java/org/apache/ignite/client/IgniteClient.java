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

import java.util.concurrent.CompletableFuture;

import org.apache.ignite.app.Ignite;
import org.apache.ignite.configuration.schemas.client.ClientView;
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

    static CompletableFuture<IgniteClient> startAsync(ClientView configuration) {
        // TODO: ClientView is immutable, which is good for us, but the name of the interface is confusing.
        // TODO: There is no easy way to access defaults from code.
        IgniteClientConfiguration cfg = new IgniteClientConfigurationImpl(
                null,
                configuration.addresses(),
                configuration.retryLimit(),
                configuration.connectTimeout(),
                0,
                0);

        return TcpIgniteClient.startAsync(cfg);
    }

    /** Client builder. */
    class Builder {
        /** Addresses. */
        private String[] addresses;

        /** Retry limit. */
        private int retryLimit;

        /** Connect timeout. */
        private int connectTimeout;

        /**
         * Sets the addresses of Ignite server nodes within a cluster. An address can be an IP address or a hostname,
         * with or without port. If port is not set then Ignite will generate multiple addresses for default port range.
         * See {@link IgniteClientConfiguration#DFLT_PORT}, {@link IgniteClientConfiguration#DFLT_PORT_RANGE}.
         *
         * @param addrs Addresses.
         */
        public Builder addresses(String... addrs) {
            addresses = addrs;

            return this;
        }

        /**
         * Sets the retry limit.
         *
         * @param retryLimit Retry limit.
         * @return This instance.
         */
        public Builder retryLimit(int retryLimit) {
            this.retryLimit = retryLimit;

            return this;
        }

        /**
         * Sets the socket connection timeout, in milliseconds.
         *
         * @param connectTimeout Socket connection timeout.
         * @return This instance.
         */
        public Builder connectTimeout(int connectTimeout) {
            this.connectTimeout = connectTimeout;

            return this;
        }

        /**
         * Builds the client.
         *
         * @return Ignite client.
         */
        public IgniteClient build() {
            // TODO: Validate values IGNITE-15164.
            return buildAsync().join();
        }

        /**
         * Builds the client.
         *
         * @return Ignite client.
         */
        public CompletableFuture<IgniteClient> buildAsync() {
            var cfg = new IgniteClientConfigurationImpl(null, addresses, retryLimit, connectTimeout, 0, 0);

            return TcpIgniteClient.startAsync(cfg);
        }
    }
}
