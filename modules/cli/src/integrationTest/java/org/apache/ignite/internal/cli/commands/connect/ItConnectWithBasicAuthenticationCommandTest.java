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

import jakarta.inject.Inject;
import java.io.IOException;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.cli.commands.ItConnectToClusterTestBase;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
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
    void failToConnectWithoutAuthentication() throws IOException {
        // Given basic authentication is not configured in config file

        // And prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // On connection error ask user to setup ssl or authentication settings. Answer 'N' to skip setup wizard
        bindAnswers("n");

        // When connect without parameters
        execute("connect");

        // Then
        assertAll(
                this::assertOutputIsEmpty,
                this::assertErrOutputIsEmpty,
                () -> assertTerminalOutputIs(AUTH_ERROR_QUESTION)
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
                () -> assertOutputContains("Connected to http://localhost:10300"),
                this::assertTerminalOutputIsEmpty
        );

        // And prompt shows username and node name
        assertThat(getPrompt()).isEqualTo("[admin:" + nodeName() + "]> ");
    }

    @Test
    void failToConnectWithWrongCredentials() throws IOException {
        // Given basic authentication is configured in config file
        configManagerProvider.setConfigFile(createIntegrationTestsConfig(), createJdbcTestsBasicSecretConfig());
        // And wrong password is provided
        setConfigProperty(CliConfigKeys.BASIC_AUTHENTICATION_PASSWORD, "wrong-password");

        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // On connection error ask user to setup ssl or authentication settings. Answer 'N' to skip setup wizard
        bindAnswers("n");

        // When connect without parameters
        execute("connect");

        // Then
        assertAll(
                this::assertOutputIsEmpty,
                this::assertErrOutputIsEmpty,
                () -> assertTerminalOutputIs(AUTH_ERROR_QUESTION)
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
                () -> assertOutputContains("Connected to http://localhost:10300"),
                this::assertTerminalOutputIsEmpty
        );

        // And prompt shows username and node name
        assertThat(getPrompt()).isEqualTo("[admin:" + nodeName() + "]> ");
    }

    @Test
    @DisplayName("Should NOT connect to cluster with incorrect password")
    void connectWithWrongAuthenticationParameters() throws IOException {
        // Given basic authentication is NOT configured in config file
        configManagerProvider.setConfigFile(createIntegrationTestsConfig());

        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // On connection error ask user to setup ssl or authentication settings. Answer 'N' to skip setup wizard
        bindAnswers("n");

        // When connect with auth parameters
        execute("connect", "--username", "admin", "--password", "wrong-password");

        // Then
        // Then
        assertAll(
                this::assertOutputIsEmpty,
                this::assertErrOutputIsEmpty,
                () -> assertTerminalOutputIs(AUTH_ERROR_QUESTION)
        );

        // And prompt is still disconnected
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");
    }

    @Test
    void connectFailIfPasswordNotDefined() throws IOException {
        // Given basic authentication is NOT configured in config file
        configManagerProvider.setConfigFile(createIntegrationTestsConfig());

        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // On connection error ask user to setup ssl or authentication settings. Answer 'N' to skip setup wizard
        bindAnswers("n");

        // When connect with auth parameters
        execute("connect", "--username", "admin", "--password", "");

        // Then
        assertAll(
                this::assertOutputIsEmpty,
                this::assertErrOutputIsEmpty,
                () -> assertTerminalOutputIs(AUTH_ERROR_QUESTION)
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
        setConfigProperty(CliConfigKeys.BASIC_AUTHENTICATION_PASSWORD, "wrong-password");

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
                        "Config saved" + System.lineSeparator() + "Connected to http://localhost:10300" + System.lineSeparator()),
                () -> assertTerminalOutputIs(REMEMBER_CREDENTIALS_QUESTION)
        );

        // And prompt shows username and node name
        assertThat(getPrompt()).isEqualTo("[admin:" + nodeName() + "]> ");
    }

    @Test
    @DisplayName("Should restore initial values in config in case of connect failed")
    void connectWithWrongAuthenticationParametersRestorePreviousCredentials() throws IOException {
        // Given basic authentication is configured in config file
        configManagerProvider.setConfigFile(createIntegrationTestsConfig(), createJdbcTestsBasicSecretConfig());

        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // On connection error ask user to setup ssl or authentication settings. Answer 'N' to skip setup wizard
        bindAnswers("n");

        // When connect with auth parameters
        execute("connect", "--username", "admin", "--password", "wrong-password");

        // Then
        assertAll(
                this::assertOutputIsEmpty,
                this::assertErrOutputIsEmpty,
                () -> assertTerminalOutputIs(AUTH_ERROR_QUESTION)
        );

        // And prompt is still disconnected
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");
        // Previous correct values restored in config
        assertThat(getConfigProperty(CliConfigKeys.BASIC_AUTHENTICATION_USERNAME)).isEqualTo("admin");
        assertThat(getConfigProperty(CliConfigKeys.BASIC_AUTHENTICATION_PASSWORD)).isEqualTo("password");
    }

    @Test
    @DisplayName("Should ask to store credentials")
    void shouldAskToStoreCredentials() throws IOException {
        // Given basic authentication is NOT configured in config file
        configManagerProvider.setConfigFile(createIntegrationTestsConfig());
        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // And answer is "y"
        bindAnswers("y");

        // And connected
        execute("connect", "--username", "admin", "--password", "password");

        // And output is
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs(
                        "Config saved" + System.lineSeparator() + "Connected to http://localhost:10300" + System.lineSeparator()),
                () -> assertTerminalOutputIs(REMEMBER_CREDENTIALS_QUESTION)
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
        setConfigProperty(CliConfigKeys.BASIC_AUTHENTICATION_PASSWORD, "wrong-password");

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
                () -> assertThat(apiClientFactory.currentSessionSettings().basicAuthenticationPassword()).isEqualTo("password"),
                () -> assertThat(getConfigProperty(CliConfigKeys.BASIC_AUTHENTICATION_PASSWORD)).isEqualTo("wrong-password"),
                () -> assertTerminalOutputIs(REMEMBER_CREDENTIALS_QUESTION)
        );
    }

    @Test
    void reconnectWithDifferentAuthenticationParameters() throws IOException {
        // Given basic authentication is configured in config file
        configManagerProvider.setConfigFile(createIntegrationTestsConfig(), createJdbcTestsBasicSecretConfig());

        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // When connect with auth parameters
        execute("connect", "--username", "admin", "--password", "password");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10300"),
                this::assertTerminalOutputIsEmpty
        );

        // And prompt shows username and node name
        assertThat(getPrompt()).isEqualTo("[admin:" + nodeName() + "]> ");

        // Should ask user to reconnect with different user and remember credentials, answer "y"
        bindAnswers("y", "y");

        // When connect with different auth parameters
        execute("connect", "--username", "admin1", "--password", "password");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs(
                        "Config saved" + System.lineSeparator() + "Connected to http://localhost:10300" + System.lineSeparator()),
                () -> assertTerminalOutputIs("You are already connected to the http://localhost:10300 as admin,"
                        + " do you want to connect as admin1? [Y/n] "
                        + REMEMBER_CREDENTIALS_QUESTION)
        );

        // And prompt shows username and node name
        assertThat(getPrompt()).isEqualTo("[admin1:" + nodeName() + "]> ");
    }

    @Test
    void reconnectWithAuthenticationParametersOverridingConfig() throws IOException {
        // Given basic authentication is configured in config file
        configManagerProvider.setConfigFile(createIntegrationTestsConfig(), createJdbcTestsBasicSecretConfig());

        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // When connect without auth parameters
        execute("connect");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10300"),
                this::assertTerminalOutputIsEmpty
        );

        // And prompt shows username and node name
        assertThat(getPrompt()).isEqualTo("[admin:" + nodeName() + "]> ");

        // Should ask user to reconnect with different user and remember credentials, answer "y"
        bindAnswers("y", "y");

        // When connect with different auth parameters
        execute("connect", "--username", "admin1", "--password", "password");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs(
                        "Config saved" + System.lineSeparator() + "Connected to http://localhost:10300" + System.lineSeparator()),
                () -> assertTerminalOutputIs("You are already connected to the http://localhost:10300 as admin,"
                        + " do you want to connect as admin1? [Y/n] "
                        + REMEMBER_CREDENTIALS_QUESTION)
        );

        // And prompt shows username and node name
        assertThat(getPrompt()).isEqualTo("[admin1:" + nodeName() + "]> ");
    }

    @Test
    void reconnectWithAuthenticationParametersFromConfig() throws IOException {
        // Given basic authentication is configured in config file
        configManagerProvider.setConfigFile(createIntegrationTestsConfig(), createJdbcTestsBasicSecretConfig());

        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // When connect with auth parameters overriding config
        execute("connect", "--username", "admin1", "--password", "password");

        // Should ask user to remember explicitly passed credentials, then to reconnect with different user, answer "y"
        bindAnswers("y", "y");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10300"),
                () -> assertTerminalOutputIs(REMEMBER_CREDENTIALS_QUESTION)
        );

        resetTerminalOutput();

        // And prompt shows username from parameters and node name
        assertThat(getPrompt()).isEqualTo("[admin1:" + nodeName() + "]> ");

        // When connect without auth parameters
        execute("connect");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10300"),
                () -> assertTerminalOutputIs("You are already connected to the http://localhost:10300 as admin1,"
                        + " do you want to connect as admin? [Y/n] ")
        );

        // And prompt shows username from config and node name
        assertThat(getPrompt()).isEqualTo("[admin:" + nodeName() + "]> ");
    }

    @Test
    void shouldStoreCredentialsFromAnswer() throws IOException {
        // Given basic authentication is NOT configured in config file
        configManagerProvider.setConfigFile(createIntegrationTestsConfig());
        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // And answer "y" to question on auth error, enter username and password and answer "y" to save config question
        bindAnswers("y", "admin", "password", "y");

        // And connected
        execute("connect", "--username", "admin", "--password", "wrong-password");

        // And output is
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs(
                        "Config saved" + System.lineSeparator() + "Connected to http://localhost:10300" + System.lineSeparator()),
                () -> assertTerminalOutputIs(AUTH_ERROR_QUESTION + USERNAME_QUESTION + PASSWORD_QUESTION + REMEMBER_CREDENTIALS_QUESTION)
        );

        // And prompt shows username and node name
        assertThat(getPrompt()).isEqualTo("[admin:" + nodeName() + "]> ");
        // And correct values are stored in config
        assertThat(getConfigProperty(CliConfigKeys.BASIC_AUTHENTICATION_USERNAME)).isEqualTo("admin");
        assertThat(getConfigProperty(CliConfigKeys.BASIC_AUTHENTICATION_PASSWORD)).isEqualTo("password");
    }


    @Test
    @DisplayName("Should ask for auth configuration connect to last connected cluster HTTPS url")
    void connectOnStartAskAuth() throws IOException {
        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // And answer to the reconnect question is "y", to the auth configuration question is "y",
        // username and password are provided and answer to save authentication is "y"
        bindAnswers("y", "y", "admin", "password", "y");

        // When asked the question
        question.askQuestionOnReplStart();

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs(
                        "Config saved" + System.lineSeparator() + "Connected to http://localhost:10300" + System.lineSeparator()),
                () -> assertTerminalOutputIs("You appear to have not connected to any node yet. "
                        + "Do you want to connect to the default node http://localhost:10300? [Y/n] "
                        + AUTH_ERROR_QUESTION + USERNAME_QUESTION + PASSWORD_QUESTION + REMEMBER_CREDENTIALS_QUESTION)
        );

        // And prompt shows user name and node name
        assertThat(getPrompt()).isEqualTo("[admin:" + nodeName() + "]> ");
        // And correct values are stored in config
        assertThat(getConfigProperty(CliConfigKeys.BASIC_AUTHENTICATION_USERNAME)).isEqualTo("admin");
        assertThat(getConfigProperty(CliConfigKeys.BASIC_AUTHENTICATION_PASSWORD)).isEqualTo("password");
    }
}
