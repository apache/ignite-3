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
import org.apache.ignite.client.SslConfiguration;

/** SSL configuration builder. */
public class SslConfigurationBuilder {
    private static String DEFAULT_KEYSTORE_TYPE = "PKCS12";

    private boolean enabled = false;

    private ClientAuthConfiguration clientAuth = ClientAuthConfiguration.NONE;

    private String keyStorePath;

    private String keyStorePassword;

    private String keyStoreType = DEFAULT_KEYSTORE_TYPE;

    private String trustStorePath;

    private String trustStorePassword;

    private String trustStoreType = DEFAULT_KEYSTORE_TYPE;

    public SslConfigurationBuilder enabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public SslConfigurationBuilder clientAuth(ClientAuthConfiguration clientAuth) {
        this.clientAuth = clientAuth;
        return this;
    }

    public SslConfigurationBuilder keyStorePath(String keyStorePath) {
        this.keyStorePath = keyStorePath;
        return this;
    }

    public SslConfigurationBuilder keyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    public SslConfigurationBuilder keyStoreType(String keyStoreType) {
        this.keyStoreType = keyStoreType;
        return this;
    }

    public SslConfigurationBuilder trustStorePath(String trustStorePath) {
        this.trustStorePath = trustStorePath;
        return this;
    }

    public SslConfigurationBuilder trustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    public SslConfigurationBuilder trustStoreType(String trustStoreType) {
        this.trustStoreType = trustStoreType;
        return this;
    }

    /** Build SslConfiguration instance. */
    public SslConfiguration build() {
        KeystoreConfigurationImpl keyStore = new KeystoreConfigurationImpl(keyStorePath, keyStorePassword, keyStoreType);
        KeystoreConfigurationImpl trustStore = new KeystoreConfigurationImpl(trustStorePath, trustStorePassword, trustStoreType);

        return new SslConfigurationImpl(enabled, clientAuth, keyStore, trustStore);
    }
}
