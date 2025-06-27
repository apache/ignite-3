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

import org.apache.ignite.internal.client.SslConfigurationImpl;
import org.jetbrains.annotations.Nullable;

/** Client SSL configuration. */
public interface SslConfiguration {
    /** If set to {@code true} then the SSL connection will be used to interact with Ignite 3 node. */
    boolean enabled();

    /** List of ciphers that will be used to setup the SSL connection. */
    @Nullable Iterable<String> ciphers();

    /** Keystore path that will be used to setup the SSL connection. */
    @Nullable String keyStorePath();

    /** Keystore password that will be used to setup the SSL connection. */
    @Nullable String keyStorePassword();

    /** Truststore path that will be used to setup the SSL connection. */
    @Nullable String trustStorePath();

    /** Truststore password that will be used to setup the SSL connection. */
    @Nullable String trustStorePassword();

    /** SSL configuration builder. */
    static Builder builder() {
        return new Builder();
    }

    /** SSL configuration builder. */
    @SuppressWarnings("PublicInnerClass")
    class Builder {
        private boolean enabled = false;

        private @Nullable Iterable<String> ciphers;

        private @Nullable String keyStorePath;

        private @Nullable String keyStorePassword;

        private @Nullable String trustStorePath;

        private @Nullable String trustStorePassword;

        /** Enabled/disabled setter. */
        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        /** Ciphers setter. */
        public Builder ciphers(@Nullable Iterable<String> ciphers) {
            this.ciphers = ciphers;
            return this;
        }

        /** Keystore path setter. */
        public Builder keyStorePath(@Nullable String keyStorePath) {
            this.keyStorePath = keyStorePath;
            return this;
        }

        /** Keystore password setter. */
        public Builder keyStorePassword(@Nullable String keyStorePassword) {
            this.keyStorePassword = keyStorePassword;
            return this;
        }

        /** Truststore path setter. */
        public Builder trustStorePath(@Nullable String trustStorePath) {
            this.trustStorePath = trustStorePath;
            return this;
        }

        /** Truststore password setter. */
        public Builder trustStorePassword(@Nullable String trustStorePassword) {
            this.trustStorePassword = trustStorePassword;
            return this;
        }

        /** Build SslConfiguration instance. */
        public SslConfiguration build() {
            return new SslConfigurationImpl(
                    enabled, ciphers,
                    keyStorePath, keyStorePassword,
                    trustStorePath, trustStorePassword
            );
        }
    }
}
