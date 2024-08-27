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

package org.apache.ignite.internal.cli.commands.questions;

import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.readClusterConfigurationWithEnabledAuth;
import static org.apache.ignite.internal.cli.commands.questions.ItConnectToSslClusterTest.KEY_STORE_QUESTION;
import static org.apache.ignite.internal.cli.commands.questions.ItConnectToSslClusterTest.RECONNECT_SSL_QUESTION;
import static org.apache.ignite.internal.cli.commands.questions.ItConnectToSslClusterTest.SSL_ERROR_QUESTION;
import static org.apache.ignite.internal.cli.commands.questions.ItConnectToSslClusterTest.TRUST_STORE_PASSWORD_QUESTION;
import static org.apache.ignite.internal.cli.commands.questions.ItConnectToSslClusterTest.TRUST_STORE_PATH_QUESTION;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.BASIC_AUTHENTICATION_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.BASIC_AUTHENTICATION_USERNAME;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_TRUST_STORE_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_TRUST_STORE_PATH;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.escapeWindowsPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.NodeConfig;
import org.apache.ignite.internal.cli.commands.ItConnectToClusterTestBase;
import org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper;
import org.apache.ignite.internal.cli.config.TestStateConfigHelper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ItConnectToSslAndAuthClusterTest extends ItConnectToClusterTestBase {
    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return NodeConfig.REST_SSL_BOOTSTRAP_CONFIG;
    }

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration(readClusterConfigurationWithEnabledAuth());
    }

    @Test
    @DisplayName("Should ask for SSL configuration connect to last connected cluster HTTPS url then ask for auth")
    void connectOnStartAskSslAfterAskAuth() throws IOException {
        // Given prompt before connect
        assertPromptIs("[disconnected]> ");

        // And default URL is HTTPS
        configManagerProvider.setConfigFile(TestConfigManagerHelper.createClusterUrlSslConfig());

        // And trust store is not configured

        // And last connected URL is equal to the default URL
        stateConfigProvider.config = TestStateConfigHelper.createLastConnectedSslDefault();

        // And answer to the reconnect question is "y", to the SSL configuration question is "y",
        // trust store path and password are provided and answer to key store configuration is "n",
        // answer to auth configuration is "y", username and password is provided and answer to save authentication is "y"
        bindAnswers("y", "y", NodeConfig.resolvedTruststorePath, NodeConfig.trustStorePassword, "n", "y", "admin", "password", "y");

        // When asked the question
        question.askQuestionOnReplStart();

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to https://localhost:10400"),
                () -> assertPromptIs("[admin:" + nodeName() + "]> "),
                () -> assertTerminalOutputIs(RECONNECT_SSL_QUESTION
                        + SSL_ERROR_QUESTION + TRUST_STORE_PATH_QUESTION + TRUST_STORE_PASSWORD_QUESTION + KEY_STORE_QUESTION
                        + AUTH_ERROR_QUESTION + USERNAME_QUESTION + PASSWORD_QUESTION + REMEMBER_CREDENTIALS_QUESTION),
                () -> assertThat(getConfigProperty(REST_TRUST_STORE_PATH)).isEqualTo(escapeWindowsPath(NodeConfig.resolvedTruststorePath)),
                () -> assertThat(getConfigProperty(REST_TRUST_STORE_PASSWORD)).isEqualTo(escapeWindowsPath(NodeConfig.trustStorePassword)),
                () -> assertThat(getConfigProperty(BASIC_AUTHENTICATION_USERNAME)).isEqualTo("admin"),
                () -> assertThat(getConfigProperty(BASIC_AUTHENTICATION_PASSWORD)).isEqualTo("password")
        );
    }

    @Test
    @DisplayName("Should ask for SSL configuration, connect to last connected cluster HTTPS url, provide wrong password")
    void connectOnStartAskSslWrongTrustPassword() throws IOException {
        // Given prompt before connect
        assertPromptIs("[disconnected]> ");

        // And default URL is HTTPS
        configManagerProvider.setConfigFile(TestConfigManagerHelper.createClusterUrlSslConfig());

        // And trust store is not configured

        // And last connected URL is equal to the default URL
        stateConfigProvider.config = TestStateConfigHelper.createLastConnectedSslDefault();

        // And answer to the reconnect question is "y", to the SSL configuration question is "y",
        // trust store path and password are provided and key store is not configured
        bindAnswers("y", "y", NodeConfig.resolvedTruststorePath, "wrong_password", "n");

        // When asked the question
        question.askQuestionOnReplStart();

        // Then
        assertAll(
                () -> assertErrOutputContains("Unexpected error" + System.lineSeparator()
                        + "Trust-store password was incorrect"),
                () -> assertPromptIs("[disconnected]> "),
                () -> assertTerminalOutputIs(RECONNECT_SSL_QUESTION
                        + SSL_ERROR_QUESTION + TRUST_STORE_PATH_QUESTION + TRUST_STORE_PASSWORD_QUESTION + KEY_STORE_QUESTION),
                () -> assertNull(getConfigProperty(REST_TRUST_STORE_PATH)),
                () -> assertNull(getConfigProperty(REST_TRUST_STORE_PASSWORD)),
                () -> assertNull(getConfigProperty(BASIC_AUTHENTICATION_USERNAME)),
                () -> assertNull(getConfigProperty(BASIC_AUTHENTICATION_PASSWORD))
        );
    }

    @Test
    @DisplayName("Should ask for SSL configuration, connect to last connected cluster HTTPS url, provide wrong path")
    void connectOnStartAskSslWrongPath() throws IOException {
        // Given prompt before connect
        assertPromptIs("[disconnected]> ");

        // And default URL is HTTPS
        configManagerProvider.setConfigFile(TestConfigManagerHelper.createClusterUrlSslConfig());

        // And trust store is not configured

        // And last connected URL is equal to the default URL
        stateConfigProvider.config = TestStateConfigHelper.createLastConnectedSslDefault();

        // And answer to the reconnect question is "y", to the SSL configuration question is "y",
        // trust store path and password are provided and key store is not configured
        String wrongPath = "wrong-path";
        bindAnswers("y", "y", wrongPath, NodeConfig.trustStorePassword, "n");

        // When asked the question
        question.askQuestionOnReplStart();

        // Then
        assertAll(
                () -> assertErrOutputContains("Unexpected error" + System.lineSeparator()
                        + "File does not exist or it does not refer to a normal file: " + wrongPath),
                () -> assertPromptIs("[disconnected]> "),
                () -> assertTerminalOutputIs(RECONNECT_SSL_QUESTION
                        + SSL_ERROR_QUESTION + TRUST_STORE_PATH_QUESTION + TRUST_STORE_PASSWORD_QUESTION + KEY_STORE_QUESTION),
                () -> assertNull(getConfigProperty(REST_TRUST_STORE_PATH)),
                () -> assertNull(getConfigProperty(REST_TRUST_STORE_PASSWORD)),
                () -> assertNull(getConfigProperty(BASIC_AUTHENTICATION_USERNAME)),
                () -> assertNull(getConfigProperty(BASIC_AUTHENTICATION_PASSWORD))
        );
    }

    @Test
    @DisplayName("Should ask for SSL configuration connect to last connected cluster HTTPS url then ask for auth, type wrong name")
    void connectOnStartAskSslAfterAskAuthWrongName() throws IOException {
        // Given prompt before connect
        assertPromptIs("[disconnected]> ");

        // And default URL is HTTPS
        configManagerProvider.setConfigFile(TestConfigManagerHelper.createClusterUrlSslConfig());

        // And trust store is not configured

        // And last connected URL is equal to the default URL
        stateConfigProvider.config = TestStateConfigHelper.createLastConnectedSslDefault();

        // And answer to the reconnect question is "y", to the SSL configuration question is "y",
        // trust store path and password are provided and key store is not configured
        bindAnswers("y", "y", NodeConfig.resolvedTruststorePath, NodeConfig.trustStorePassword, "n", "y", "wrong_name", "password");

        // When asked the question
        question.askQuestionOnReplStart();

        // Then
        assertAll(
                () -> assertErrOutputIs("Authentication error" + System.lineSeparator()
                        + "Could not connect to node with URL https://localhost:10400. "
                        + "Check authentication configuration or provided username/password"
                        + System.lineSeparator()),
                () -> assertPromptIs("[disconnected]> "),
                () -> assertTerminalOutputIs(RECONNECT_SSL_QUESTION
                        + SSL_ERROR_QUESTION + TRUST_STORE_PATH_QUESTION + TRUST_STORE_PASSWORD_QUESTION + KEY_STORE_QUESTION
                        + AUTH_ERROR_QUESTION + USERNAME_QUESTION + PASSWORD_QUESTION),
                () -> assertThat(getConfigProperty(REST_TRUST_STORE_PATH)).isEqualTo(escapeWindowsPath(NodeConfig.resolvedTruststorePath)),
                () -> assertThat(getConfigProperty(REST_TRUST_STORE_PASSWORD)).isEqualTo(escapeWindowsPath(NodeConfig.trustStorePassword)),
                () -> assertNull(getConfigProperty(BASIC_AUTHENTICATION_USERNAME)),
                () -> assertNull(getConfigProperty(BASIC_AUTHENTICATION_PASSWORD))
        );
    }

    @Test
    @DisplayName("Should ask for SSL configuration connect to last connected cluster HTTPS url")
    void connectWithCredentialsFailAskSsl() throws IOException {
        // Given prompt before connect
        assertPromptIs("[disconnected]> ");

        // And default URL is HTTPS
        configManagerProvider.setConfigFile(TestConfigManagerHelper.createClusterUrlSslConfig());

        // And trust store is not configured

        // And last connected URL is equal to the default URL
        stateConfigProvider.config = TestStateConfigHelper.createLastConnectedSslDefault();

        // And answer to the reconnect question is "y", to the SSL configuration question is "y",
        // trust store path and password are provided and key store is not configured
        // and answer to save authentication is "y"
        bindAnswers("y", NodeConfig.resolvedTruststorePath, NodeConfig.trustStorePassword, "n", "y");

        // When connect with auth parameters
        execute("connect", "--username", "admin", "--password", "password");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to https://localhost:10400"),
                () -> assertPromptIs("[admin:" + nodeName() + "]> "),
                () -> assertTerminalOutputIs(
                        SSL_ERROR_QUESTION + TRUST_STORE_PATH_QUESTION + TRUST_STORE_PASSWORD_QUESTION + KEY_STORE_QUESTION
                        + REMEMBER_CREDENTIALS_QUESTION),
                () -> assertThat(getConfigProperty(REST_TRUST_STORE_PATH)).isEqualTo(escapeWindowsPath(NodeConfig.resolvedTruststorePath)),
                () -> assertThat(getConfigProperty(REST_TRUST_STORE_PASSWORD)).isEqualTo(escapeWindowsPath(NodeConfig.trustStorePassword)),
                () -> assertThat(getConfigProperty(BASIC_AUTHENTICATION_USERNAME)).isEqualTo("admin"),
                () -> assertThat(getConfigProperty(BASIC_AUTHENTICATION_PASSWORD)).isEqualTo("password")
        );
    }

    @Test
    @DisplayName("Should ask for SSL configuration connect to last connected cluster HTTPS url then ask for auth")
    void connectWithWrongCredentialsFailAskSsl() throws IOException {
        // Given prompt before connect
        assertPromptIs("[disconnected]> ");

        // And default URL is HTTPS
        configManagerProvider.setConfigFile(TestConfigManagerHelper.createClusterUrlSslConfig());

        // And trust store is not configured

        // And last connected URL is equal to the default URL
        stateConfigProvider.config = TestStateConfigHelper.createLastConnectedSslDefault();

        // And answer to the SSL configuration question is "y", trust store path and password are provided
        // and answer to key store configuration is "n", answer to auth configuration is "y", username and password is provided
        // and answer to save authentication is "y"
        bindAnswers("y", NodeConfig.resolvedTruststorePath, NodeConfig.trustStorePassword, "n", "y", "admin", "password", "y");

        // When connect with auth parameters
        execute("connect", "--username", "admin", "--password", "wrong-password");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to https://localhost:10400"),
                () -> assertPromptIs("[admin:" + nodeName() + "]> "),
                () -> assertTerminalOutputIs(
                        SSL_ERROR_QUESTION + TRUST_STORE_PATH_QUESTION + TRUST_STORE_PASSWORD_QUESTION + KEY_STORE_QUESTION
                        + AUTH_ERROR_QUESTION + USERNAME_QUESTION + PASSWORD_QUESTION + REMEMBER_CREDENTIALS_QUESTION),
                () -> assertThat(getConfigProperty(REST_TRUST_STORE_PATH)).isEqualTo(escapeWindowsPath(NodeConfig.resolvedTruststorePath)),
                () -> assertThat(getConfigProperty(REST_TRUST_STORE_PASSWORD)).isEqualTo(escapeWindowsPath(NodeConfig.trustStorePassword)),
                () -> assertThat(getConfigProperty(BASIC_AUTHENTICATION_USERNAME)).isEqualTo("admin"),
                () -> assertThat(getConfigProperty(BASIC_AUTHENTICATION_PASSWORD)).isEqualTo("password")
        );
    }

    @Test
    @DisplayName("Should ask for SSL configuration connect to last connected cluster HTTPS url then ask for auth on plain connect command")
    void connectWithoutCredentialsFailAskSslAfterAskAuth() throws IOException {
        // Given prompt before connect
        assertPromptIs("[disconnected]> ");

        // And default URL is HTTPS
        configManagerProvider.setConfigFile(TestConfigManagerHelper.createClusterUrlSslConfig());

        // And trust store is not configured

        // And last connected URL is equal to the default URL
        stateConfigProvider.config = TestStateConfigHelper.createLastConnectedSslDefault();

        // And answer to the SSL configuration question is "y", trust store path and password are provided
        // and answer to key store configuration is "n", answer to auth configuration is "y", username and password is provided
        // and answer to save authentication is "y"
        bindAnswers("y", NodeConfig.resolvedTruststorePath, NodeConfig.trustStorePassword, "n", "y", "admin", "password", "y");

        // When connect with auth parameters
        execute("connect");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to https://localhost:10400"),
                () -> assertPromptIs("[admin:" + nodeName() + "]> "),
                () -> assertTerminalOutputIs(
                        SSL_ERROR_QUESTION + TRUST_STORE_PATH_QUESTION + TRUST_STORE_PASSWORD_QUESTION + KEY_STORE_QUESTION
                        + AUTH_ERROR_QUESTION + USERNAME_QUESTION + PASSWORD_QUESTION + REMEMBER_CREDENTIALS_QUESTION),
                () -> assertThat(getConfigProperty(REST_TRUST_STORE_PATH)).isEqualTo(escapeWindowsPath(NodeConfig.resolvedTruststorePath)),
                () -> assertThat(getConfigProperty(REST_TRUST_STORE_PASSWORD)).isEqualTo(escapeWindowsPath(NodeConfig.trustStorePassword)),
                () -> assertThat(getConfigProperty(BASIC_AUTHENTICATION_USERNAME)).isEqualTo("admin"),
                () -> assertThat(getConfigProperty(BASIC_AUTHENTICATION_PASSWORD)).isEqualTo("password")
        );
    }
}
