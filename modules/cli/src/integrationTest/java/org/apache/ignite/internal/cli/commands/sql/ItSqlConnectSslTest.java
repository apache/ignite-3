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

package org.apache.ignite.internal.cli.commands.sql;

import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.internal.NodeConfig;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ItSqlConnectSslTest extends CliSqlConnectCommandTestBase {
    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return NodeConfig.CLIENT_CONNECTOR_SSL_BOOTSTRAP_CONFIG;
    }

    @Test
    @DisplayName("Should succeed after connect with SSL configured")
    void jdbcOkWithSslConfiguredAfterConnect() {
        // Given SSL configured in config file
        setConfigProperty(CliConfigKeys.JDBC_TRUST_STORE_PATH, NodeConfig.resolvedTruststorePath);
        setConfigProperty(CliConfigKeys.JDBC_TRUST_STORE_PASSWORD, NodeConfig.trustStorePassword);
        setConfigProperty(CliConfigKeys.JDBC_KEY_STORE_PATH, NodeConfig.resolvedKeystorePath);
        setConfigProperty(CliConfigKeys.JDBC_KEY_STORE_PASSWORD, NodeConfig.keyStorePassword);
        setConfigProperty(CliConfigKeys.JDBC_CLIENT_AUTH, "require");

        // Given connected state
        execute("connect");

        // When
        execute("sql", "select * from person");

        // Then the query is executed successfully
        assertAll(
                this::assertExitCodeIsZero,
                this::assertOutputIsNotEmpty,
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Should fail after connect without SSL configured")
    void jdbcFailWithoutSslConfiguredAfterConnect() {
        // Given SSL is not configured in config file

        // Given connected state
        execute("connect");

        // When
        execute("sql", "select * from person");

        // Then the query is failed
        assertAll(
                () -> assertOutputIs("Connected to " + NODE_URL + System.lineSeparator()),
                () -> assertErrOutputContains("Connection failed"),
                () -> assertErrOutputContains("Handshake error")
        );
    }
}
