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

/** SSL configuration. */
public class SslConfigurationImpl implements SslConfiguration {
    private final boolean enabled;

    private final ClientAuthenticationMode clientAuth;

    private final @Nullable Iterable<String> ciphers;

    private final @Nullable String keyStorePath;

    private final @Nullable String keyStorePassword;

    private final @Nullable String trustStorePath;

    private final @Nullable String trustStorePassword;

    /** Main constructor. */
    SslConfigurationImpl(
            boolean enabled,
            ClientAuthenticationMode clientAuth,
            @Nullable Iterable<String> ciphers,
            @Nullable String keyStorePath,
            @Nullable String keyStorePassword,
            @Nullable String trustStorePath,
            @Nullable String trustStorePassword
    ) {
        this.enabled = enabled;
        this.clientAuth = clientAuth;
        this.ciphers = ciphers;
        this.keyStorePath = keyStorePath;
        this.keyStorePassword = keyStorePassword;
        this.trustStorePath = trustStorePath;
        this.trustStorePassword = trustStorePassword;
    }

    /** {@inheritDoc} */
    @Override
    public boolean enabled() {
        return enabled;
    }

    /** {@inheritDoc} */
    @Override
    public ClientAuthenticationMode clientAuthenticationMode() {
        return clientAuth;
    }

    @Override
    public @Nullable Iterable<String> ciphers() {
        return ciphers;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String keyStorePath() {
        return keyStorePath;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String keyStorePassword() {
        return keyStorePassword;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String trustStorePath() {
        return trustStorePath;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String trustStorePassword() {
        return trustStorePassword;
    }

}
