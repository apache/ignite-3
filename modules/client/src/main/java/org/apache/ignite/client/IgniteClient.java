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

import org.apache.ignite.app.Ignite;
import org.apache.ignite.client.internal.TcpIgniteClient;

import java.util.concurrent.CompletableFuture;

// TODO: Builder pattern for configuration - see examples of Lettuce, Mongo clients.
// * "Ignition" interface in ignite-api should be removed
public class IgniteClient {
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String[] addresses;

        public Ignite build() {
            // TODO: Validate values.
            return buildAsync().join();
        }

        public Builder addresses(String... addrs) {
            addresses = addrs;

            return this;
        }

        public CompletableFuture<Ignite> buildAsync() {
            // TODO: Async connect.
            var cfg = new IgniteClientConfigurationImpl(null, addresses, 0);

            return CompletableFuture.completedFuture(new TcpIgniteClient(cfg));
        }
    }

    private static class IgniteClientConfigurationImpl implements IgniteClientConfiguration {
        private final IgniteClientAddressFinder addressFinder;

        private final String[] addresses;

        private final int retryLimit;

        public IgniteClientConfigurationImpl(IgniteClientAddressFinder addressFinder, String[] addresses, int retryLimit) {
            this.addressFinder = addressFinder;
            this.addresses = addresses;
            this.retryLimit = retryLimit;
        }

        @Override public IgniteClientAddressFinder getAddressesFinder() {
            return addressFinder;
        }

        @Override public String[] getAddresses() {
            // TODO: Defensive copy?
            return addresses;
        }

        @Override public int getRetryLimit() {
            return retryLimit;
        }
    }
}
