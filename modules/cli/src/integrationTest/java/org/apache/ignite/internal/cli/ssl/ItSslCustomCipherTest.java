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

package org.apache.ignite.internal.cli.ssl;

import static org.apache.ignite.internal.NodeConfig.restSslBootstrapConfig;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.internal.NodeConfig;
import org.apache.ignite.internal.cli.commands.CliCommandTestNotInitializedIntegrationBase;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.junit.jupiter.api.Test;

/** Tests for REST SSL. */
public class ItSslCustomCipherTest extends CliCommandTestNotInitializedIntegrationBase {
    private static final String CIPHER1 = "TLS_AES_256_GCM_SHA384";
    private static final String CIPHER2 = "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384";

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return restSslBootstrapConfig(CIPHER1);
    }

    @Test
    void compatibleCiphers() {
        // When REST SSL is enabled
        setConfigProperty(CliConfigKeys.REST_TRUST_STORE_PATH, NodeConfig.resolvedTruststorePath);
        setConfigProperty(CliConfigKeys.REST_TRUST_STORE_PASSWORD, NodeConfig.trustStorePassword);
        setConfigProperty(CliConfigKeys.REST_KEY_STORE_PATH, NodeConfig.resolvedKeystorePath);
        setConfigProperty(CliConfigKeys.REST_KEY_STORE_PASSWORD, NodeConfig.keyStorePassword);

        // And explicitly set the same cipher as for the node
        setConfigProperty(CliConfigKeys.REST_CIPHERS, CIPHER1);

        // And connect via HTTPS
        connect("https://localhost:10400");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to https://localhost:10400")
        );
    }

    @Test
    void incompatibleCiphers() {
        // When REST SSL is enabled
        setConfigProperty(CliConfigKeys.REST_TRUST_STORE_PATH, NodeConfig.resolvedTruststorePath);
        setConfigProperty(CliConfigKeys.REST_TRUST_STORE_PASSWORD, NodeConfig.trustStorePassword);
        setConfigProperty(CliConfigKeys.REST_KEY_STORE_PATH, NodeConfig.resolvedKeystorePath);
        setConfigProperty(CliConfigKeys.REST_KEY_STORE_PASSWORD, NodeConfig.keyStorePassword);

        // And explicitly set cipher different from the node
        setConfigProperty(CliConfigKeys.REST_CIPHERS, CIPHER2);

        // And connect via HTTPS
        connect("https://localhost:10400");

        // Then
        assertAll(
                () -> assertErrOutputContains("SSL error"),
                this::assertOutputIsEmpty
        );
    }
}
