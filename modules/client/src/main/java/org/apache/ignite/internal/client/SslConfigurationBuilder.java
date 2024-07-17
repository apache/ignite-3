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

import org.apache.ignite.client.ClientAuthenticationMode;
import org.apache.ignite.client.SslConfiguration;
import org.jetbrains.annotations.Nullable;

/** SSL configuration builder. */
public class SslConfigurationBuilder {

    private boolean enabled = false;

    private ClientAuthenticationMode clientAuth = ClientAuthenticationMode.NONE;

    private @Nullable Iterable<String> ciphers;

    private @Nullable String keyStorePath;

    private @Nullable String keyStorePassword;

    private @Nullable String trustStorePath;

    private @Nullable String trustStorePassword;

    /** Enabled/disabled setter. */
    public SslConfigurationBuilder enabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /** SSL client authentication setter. */
    public SslConfigurationBuilder clientAuth(@Nullable ClientAuthenticationMode clientAuth) {
        if (clientAuth == null) {
            this.clientAuth = ClientAuthenticationMode.NONE;
            return this;
        }

        this.clientAuth = clientAuth;
        return this;
    }

    /** Ciphers setter. */
    public SslConfigurationBuilder ciphers(@Nullable Iterable<String> ciphers) {
        this.ciphers = ciphers;
        return this;
    }

    /** Keystore path setter. */
    public SslConfigurationBuilder keyStorePath(@Nullable String keyStorePath) {
        this.keyStorePath = keyStorePath;
        return this;
    }

    /** Keystore password setter. */
    public SslConfigurationBuilder keyStorePassword(@Nullable String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    /** Truststore path setter. */
    public SslConfigurationBuilder trustStorePath(@Nullable String trustStorePath) {
        this.trustStorePath = trustStorePath;
        return this;
    }

    /** Truststore password setter. */
    public SslConfigurationBuilder trustStorePassword(@Nullable String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    /** Build SslConfiguration instance. */
    public SslConfiguration build() {
        return new SslConfigurationImpl(
                enabled, clientAuth, ciphers,
                keyStorePath, keyStorePassword,
                trustStorePath, trustStorePassword
        );
    }
}
