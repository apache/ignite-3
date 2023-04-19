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

import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createIntegrationTests;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createJdbcTestsBasicSecret;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.List;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.NodeConfig;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.security.AuthenticationConfig;
import org.apache.ignite.security.BasicAuthenticationProviderConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ItSqlConnectSslBasicTest extends CliSqlConnectCommandTestBase {
    @Override
    protected String nodeBootstrapConfigTemplate() {
        return NodeConfig.CLIENT_CONNECTOR_SSL_BOOTSTRAP_CONFIG;
    }

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        builder.authenticationConfig(new AuthenticationConfig(
                true,
                List.of(new BasicAuthenticationProviderConfig("basic", "usr", "pwd")))
        );
    }

    @Test
    @DisplayName("Should succeed after connect with SSL and basic auth configured")
    void jdbcOkWithSslAndBasicConfiguredAfterConnect() {
        // Given SSL and basic auth is configured in config file
        configManagerProvider.setConfigFile(createIntegrationTests(), createJdbcTestsBasicSecret());
        configManagerProvider.configManager.setProperty(CliConfigKeys.JDBC_TRUST_STORE_PATH.value(), NodeConfig.resolvedTruststorePath);
        configManagerProvider.configManager.setProperty(CliConfigKeys.JDBC_TRUST_STORE_PASSWORD.value(), NodeConfig.trustStorePassword);
        configManagerProvider.configManager.setProperty(CliConfigKeys.JDBC_KEY_STORE_PATH.value(), NodeConfig.resolvedKeystorePath);
        configManagerProvider.configManager.setProperty(CliConfigKeys.JDBC_KEY_STORE_PASSWORD.value(), NodeConfig.keyStorePassword);
        configManagerProvider.configManager.setProperty(CliConfigKeys.JDBC_CLIENT_AUTH.value(), "require");

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
    @DisplayName("Should fail after connect without SSL and basic auth configured")
    void jdbcFailWithoutBasicConfiguredAfterConnect() {
        // Given neither SSL nor basic auth is configured in config file

        // Given connected state
        execute("connect");

        // When
        execute("sql", "select * from person");

        // Then the query is failed
        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Connection failed"),
                () -> assertErrOutputContains("Handshake error")
        );
    }
}
