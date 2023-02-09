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
    private static final String DEFAULT_KEYSTORE_TYPE = "PKCS12";

    private boolean enabled = false;

    private ClientAuthenticationMode clientAuth = ClientAuthenticationMode.NONE;

    private @Nullable String keyStorePath;

    private @Nullable String keyStorePassword;

    private String keyStoreType = DEFAULT_KEYSTORE_TYPE;

    private @Nullable String trustStorePath;

    private @Nullable String trustStorePassword;

    private String trustStoreType = DEFAULT_KEYSTORE_TYPE;

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

    /** Keystore type setter. If set to {@code null} then the default PKCS12 type is used. */
    public SslConfigurationBuilder keyStoreType(@Nullable String keyStoreType) {
        if (keyStoreType == null) {
            this.keyStoreType = DEFAULT_KEYSTORE_TYPE;
            return this;
        }

        this.keyStoreType = keyStoreType;
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

    /** Truststore type setter. If set to {@code null} then the default PKCS12 type is used. */
    public SslConfigurationBuilder trustStoreType(@Nullable String trustStoreType) {
        if (trustStoreType == null) {
            this.trustStoreType = DEFAULT_KEYSTORE_TYPE;
            return this;
        }

        this.trustStoreType = trustStoreType;
        return this;
    }

    /** Build SslConfiguration instance. */
    public SslConfiguration build() {
        return new SslConfigurationImpl(
                enabled, clientAuth, keyStorePath, keyStorePassword, keyStoreType, trustStorePath, trustStorePassword, trustStoreType
        );
    }
}
