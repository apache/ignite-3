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

import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.internal.NodeConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests for SSL. */
public class ItSslTest extends CliSslNotInitializedIntegrationTestBase {

    @Test
    @DisplayName("Should get SSL error, when connect to secured node without SSL settings")
    void connectToSecuredNodeWithoutSslSettings() {
        // When connect via HTTPS without SSL
        execute("connect", "https://localhost:10401");

        // Then
        assertAll(
                () -> assertErrOutputContains("SSL error"),
                this::assertOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Should connect to cluster with given url")
    void connectToSecuredNode() {
        // When set up ssl configuration
        execute("cli", "config", "set", "ignite.rest.key-store.path=" + NodeConfig.resolvedKeystorePath);
        execute("cli", "config", "set", "ignite.rest.key-store.password=" + NodeConfig.keyStorePassword);
        execute("cli", "config", "set", "ignite.rest.trust-store.path=" + NodeConfig.resolvedTruststorePath);
        execute("cli", "config", "set", "ignite.rest.trust-store.password=" + NodeConfig.trustStorePassword);
        resetOutput();

        // And connect via HTTPS
        execute("connect", "https://localhost:10401");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to https://localhost:10401")
        );
    }

}
