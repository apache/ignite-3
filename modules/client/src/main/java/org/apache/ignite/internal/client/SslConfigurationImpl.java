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

import org.apache.ignite.client.ClientAuthConfiguration;
import org.apache.ignite.client.KeystoreConfiguration;
import org.apache.ignite.client.SslConfiguration;
import org.jetbrains.annotations.Nullable;

/** SSL configuration. */
public class SslConfigurationImpl implements SslConfiguration {
    private final boolean enabled;

    private final ClientAuthConfiguration clientAuth;

    private final KeystoreConfiguration keyStore;

    private final KeystoreConfiguration trustStore;

    /** Main constructor. */
    SslConfigurationImpl(boolean enabled, ClientAuthConfiguration clientAuth,
            KeystoreConfiguration keyStore, KeystoreConfiguration trustStore) {
        this.enabled = enabled;
        this.clientAuth = clientAuth;
        this.keyStore = keyStore;
        this.trustStore = trustStore;
    }

    /** {@inheritDoc} */
    @Override
    public boolean enabled() {
        return enabled;
    }

    /** {@inheritDoc} */
    @Override
    public ClientAuthConfiguration clientAuth() {
        return clientAuth;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable KeystoreConfiguration keyStore() {
        return keyStore;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable KeystoreConfiguration trustStore() {
        return trustStore;
    }
}
