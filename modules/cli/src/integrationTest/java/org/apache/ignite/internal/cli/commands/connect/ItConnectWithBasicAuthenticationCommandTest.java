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

package org.apache.ignite.internal.cli.commands.connect;

import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createIntegrationTestsConfig;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createJdbcTestsBasicSecretConfig;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.readClusterConfigurationWithEnabledAuth;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.cli.commands.ItConnectToClusterTestBase;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.junit.jupiter.api.Test;

class ItConnectWithBasicAuthenticationCommandTest extends ItConnectToClusterTestBase {
    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration(readClusterConfigurationWithEnabledAuth());
    }

    @Test
    void failToConnectWithoutAuthentication() {
        // Given basic authentication is not configured in config file

        // And prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // When connect without parameters
        execute("connect");

        // Then
        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputIs("Authentication error" + System.lineSeparator()
                        + "Could not connect to node with URL http://localhost:10300. Check authentication configuration"
                        + System.lineSeparator())
        );
        // And prompt is still disconnected
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");
    }

    @Test
    void connectWithAuthentication() {
        // Given basic authentication is configured in config file
        configManagerProvider.setConfigFile(createIntegrationTestsConfig(), createJdbcTestsBasicSecretConfig());

        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // When connect without parameters
        execute("connect");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10300")
        );

        // And prompt shows user name and node name
        assertThat(getPrompt()).isEqualTo("[admin:" + nodeName() + "]> ");
    }

    @Test
    void failToConnectWithWrongCredentials() {
        // Given basic authentication is configured in config file
        configManagerProvider.setConfigFile(createIntegrationTestsConfig(), createJdbcTestsBasicSecretConfig());
        // And wrong password is provided
        configManagerProvider.configManager.setProperty(CliConfigKeys.Constants.BASIC_AUTHENTICATION_PASSWORD, "wrong-password");

        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // When connect without parameters
        execute("connect");

        // Then
        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputIs("Authentication error" + System.lineSeparator()
                        + "Could not connect to node with URL http://localhost:10300. Check authentication configuration"
                        + System.lineSeparator())
        );
        // And prompt is still disconnected
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");
    }
}
