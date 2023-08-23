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
import static org.junit.jupiter.api.Assertions.assertEquals;

import jakarta.inject.Inject;
import java.io.IOException;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.cli.commands.ItConnectToClusterTestBase;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.CliConfigKeys.Constants;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ItConnectWithBasicAuthenticationCommandTest extends ItConnectToClusterTestBase {

    @Inject
    private ApiClientFactory apiClientFactory;

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
                        + "Could not connect to node with URL http://localhost:10300. "
                        + "Check authentication configuration or provided username/password"
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

        // And prompt shows username and node name
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
                        + "Could not connect to node with URL http://localhost:10300. "
                        + "Check authentication configuration or provided username/password"
                        + System.lineSeparator())
        );
        // And prompt is still disconnected
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");
    }

    @Test
    @DisplayName("Should connect to cluster with username/password")
    void connectWithAuthenticationParameters() throws IOException {
        // Given basic authentication is NOT configured in config file
        configManagerProvider.setConfigFile(createIntegrationTestsConfig(), createJdbcTestsBasicSecretConfig());

        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // When connect with auth parameters
        execute("connect", "--username", "admin", "--password", "password");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10300")
        );

        // And prompt shows username and node name
        assertThat(getPrompt()).isEqualTo("[admin:" + nodeName() + "]> ");
    }

    @Test
    @DisplayName("Should NOT connect to cluster with incorrect password")
    void connectWithWrongAuthenticationParameters() {
        // Given basic authentication is NOT configured in config file
        configManagerProvider.setConfigFile(createIntegrationTestsConfig());

        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // When connect with auth parameters
        execute("connect", "--username", "admin", "--password", "wrong-password");

        // Then
        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputIs("Authentication error" + System.lineSeparator()
                        + "Could not connect to node with URL http://localhost:10300. "
                        + "Check authentication configuration or provided username/password"
                        + System.lineSeparator())
        );
        // And prompt is still disconnected
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");
    }

    @Test
    void connectFailIfPasswordNotDefined() {
        // Given basic authentication is NOT configured in config file
        configManagerProvider.setConfigFile(createIntegrationTestsConfig());

        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // When connect with auth parameters
        execute("connect", "--username", "admin", "--password", "");

        // Then
        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputIs("Authentication error" + System.lineSeparator()
                        + "Could not connect to node with URL http://localhost:10300. "
                        + "Check authentication configuration or provided username/password"
                        + System.lineSeparator())
        );
        // And prompt is still disconnected
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");
    }

    @Test
    @DisplayName("Should connect to cluster with incorrect password in config but correct in command")
    void connectWithWrongAuthenticationParametersInConfig() throws IOException {
        // Given basic authentication is configured in config file
        configManagerProvider.setConfigFile(createIntegrationTestsConfig(), createJdbcTestsBasicSecretConfig());
        // And wrong password is in config
        configManagerProvider.configManager.setProperty(CliConfigKeys.Constants.BASIC_AUTHENTICATION_PASSWORD, "wrong-password");

        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // And answer is "y"
        bindAnswers("y");

        // When connect with auth parameters
        execute("connect", "--username", "admin", "--password", "password");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs(
                        "Config saved" + System.lineSeparator() + "Connected to http://localhost:10300" + System.lineSeparator())
        );

        // And prompt shows username and node name
        assertThat(getPrompt()).isEqualTo("[admin:" + nodeName() + "]> ");
    }

    @Test
    @DisplayName("Should restore initial values in config in case of connect failed")
    void connectWithWrongAuthenticationParametersRestorePreviousCredentials() {
        // Given basic authentication is configured in config file
        configManagerProvider.setConfigFile(createIntegrationTestsConfig(), createJdbcTestsBasicSecretConfig());

        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // When connect with auth parameters
        execute("connect", "--username", "admin", "--password", "wrong-password");

        // Then
        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputIs("Authentication error" + System.lineSeparator()
                        + "Could not connect to node with URL http://localhost:10300. "
                        + "Check authentication configuration or provided username/password"
                        + System.lineSeparator())
        );
        // And prompt is still disconnected
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");
        //Previous correct values restored in config
        assertEquals("admin", configManagerProvider.get().getCurrentProperty(Constants.BASIC_AUTHENTICATION_USERNAME));
        assertEquals("password", configManagerProvider.get().getCurrentProperty(Constants.BASIC_AUTHENTICATION_PASSWORD));
    }

    @Test
    @DisplayName("Should ask to store credentials")
    void shouldAskToStoreCredentials() throws IOException {
        // Given basic authentication is NOT configured in config file
        configManagerProvider.setConfigFile(createIntegrationTestsConfig());
        // Given prompt before connect
        String promptBefore = getPrompt();
        assertThat(promptBefore).isEqualTo("[disconnected]> ");

        // And answer is "y"
        bindAnswers("y");

        // And connected
        execute("connect", "--username", "admin", "--password", "password");

        // And output is
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs(
                        "Config saved" + System.lineSeparator() + "Connected to http://localhost:10300" + System.lineSeparator())
        );

        // And prompt shows username and node name
        assertThat(getPrompt()).isEqualTo("[admin:" + nodeName() + "]> ");
    }

    @Test
    @DisplayName("Should create correct api client even if user doesn't store credentials in settings.")
    void sessionListenersShouldBeInvokedWithCorrectCredentials() throws IOException {
        // Given basic authentication is configured in config file
        configManagerProvider.setConfigFile(createIntegrationTestsConfig(), createJdbcTestsBasicSecretConfig());
        // And wrong password is in config
        configManagerProvider.configManager.setProperty(CliConfigKeys.Constants.BASIC_AUTHENTICATION_PASSWORD, "wrong-password");

        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // And answer is "n"
        bindAnswers("n");

        // When connect with auth parameters
        execute("connect", "--username", "admin", "--password", "password");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertThat(getPrompt()).isEqualTo("[admin:" + nodeName() + "]> "),
                () -> assertEquals("password", apiClientFactory.currentSessionSettings().basicAuthenticationPassword()),
                () -> assertEquals("wrong-password",
                        configManagerProvider.get().getCurrentProperty(Constants.BASIC_AUTHENTICATION_PASSWORD))
        );
    }
}
