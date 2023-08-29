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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.escapeWindowsPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.NodeConfig;
import org.apache.ignite.internal.cli.commands.ItConnectToClusterTestBase;
import org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.TestStateConfigHelper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ItConnectToSslAndAuthClusterTest extends ItConnectToClusterTestBase {
    @Override
    protected String nodeBootstrapConfigTemplate() {
        return NodeConfig.REST_SSL_BOOTSTRAP_CONFIG;
    }

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration(readClusterConfigurationWithEnabledAuth());
    }

    @Test
    @DisplayName("Should ask for SSL configuration connect to last connected cluster HTTPS url than ask for auth")
    void connectOnStartAskSslAfterAskAuth() throws IOException {
        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // And default URL is HTTPS
        configManagerProvider.setConfigFile(TestConfigManagerHelper.createClusterUrlSslConfig());

        // And trust store is not configured

        // And last connected URL is equal to the default URL
        stateConfigProvider.config = TestStateConfigHelper.createLastConnectedSslDefault();

        // And answer to the reconnect question is "y", to the SSL configuration question is "y",
        // trust store path and password are provided and key store is not configured
        bindAnswers("y", "y", NodeConfig.resolvedTruststorePath, NodeConfig.trustStorePassword, "n", "y", "admin", "password");

        // When asked the question
        question.askQuestionOnReplStart();

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to https://localhost:10400")
        );
        // And prompt is changed to connect
        assertThat(getPrompt()).isEqualTo("[admin:" + nodeName() + "]> ");

        assertAll(() -> assertThat(configManagerProvider.get().getCurrentProperty(CliConfigKeys.REST_TRUST_STORE_PATH.value()))
                        .isEqualTo(escapeWindowsPath(NodeConfig.resolvedTruststorePath)),
                () -> assertThat(configManagerProvider.get().getCurrentProperty(CliConfigKeys.REST_TRUST_STORE_PASSWORD.value()))
                        .isEqualTo(escapeWindowsPath(NodeConfig.trustStorePassword)),

                () -> assertThat(configManagerProvider.get().getCurrentProperty(CliConfigKeys.BASIC_AUTHENTICATION_USERNAME.value()))
                        .isEqualTo("admin"),
                () -> assertThat(configManagerProvider.get().getCurrentProperty(CliConfigKeys.BASIC_AUTHENTICATION_PASSWORD.value()))
                        .isEqualTo("password")
        );
    }

    @Test
    @DisplayName("Should ask for SSL configuration connect to last connected cluster HTTPS url, provide wrong password")
    void connectOnStartAskSslWrongTrustPassword() throws IOException {
        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // And default URL is HTTPS
        configManagerProvider.setConfigFile(TestConfigManagerHelper.createClusterUrlSslConfig());

        // And trust store is not configured

        // And last connected URL is equal to the default URL
        stateConfigProvider.config = TestStateConfigHelper.createLastConnectedSslDefault();

        // And answer to the reconnect question is "y", to the SSL configuration question is "y",
        // trust store path and password are provided and key store is not configured
        bindAnswers("y", "y", NodeConfig.resolvedTruststorePath, "wrong_password", "n", "y", "admin", "password");

        // When asked the question
        question.askQuestionOnReplStart();

        // Then
        assertAll(
                () -> assertErrOutputContains("Unexpected error" + System.lineSeparator()
                        + "Trust-store password was incorrect")
        );
        // And prompt is disconnected
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        assertAll(() -> assertNull(configManagerProvider.get().getCurrentProperty(CliConfigKeys.REST_TRUST_STORE_PATH.value())),
                () -> assertNull(configManagerProvider.get().getCurrentProperty(CliConfigKeys.REST_TRUST_STORE_PASSWORD.value())),
                () -> assertNull(configManagerProvider.get().getCurrentProperty(CliConfigKeys.BASIC_AUTHENTICATION_USERNAME.value())),
                () -> assertNull(configManagerProvider.get().getCurrentProperty(CliConfigKeys.BASIC_AUTHENTICATION_PASSWORD.value()))
        );
    }

    @Test
    @DisplayName("Should ask for SSL configuration connect to last connected cluster HTTPS url, provide wrong path")
    void connectOnStartAskSslWrongPath() throws IOException {
        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // And default URL is HTTPS
        configManagerProvider.setConfigFile(TestConfigManagerHelper.createClusterUrlSslConfig());

        // And trust store is not configured

        // And last connected URL is equal to the default URL
        stateConfigProvider.config = TestStateConfigHelper.createLastConnectedSslDefault();

        // And answer to the reconnect question is "y", to the SSL configuration question is "y",
        // trust store path and password are provided and key store is not configured
        String wrongPath = "wrong-path";
        bindAnswers("y", "y", wrongPath, NodeConfig.trustStorePassword, "n", "y", "admin", "password");

        // When asked the question
        question.askQuestionOnReplStart();

        // Then
        assertAll(
                () -> assertErrOutputContains("Unexpected error" + System.lineSeparator()
                        + "File does not exist or it does not refer to a normal file: " + wrongPath)
        );
        // And prompt is disconnected
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        assertAll(() -> assertNull(configManagerProvider.get().getCurrentProperty(CliConfigKeys.REST_TRUST_STORE_PATH.value())),
                () -> assertNull(configManagerProvider.get().getCurrentProperty(CliConfigKeys.REST_TRUST_STORE_PASSWORD.value())),
                () -> assertNull(configManagerProvider.get().getCurrentProperty(CliConfigKeys.BASIC_AUTHENTICATION_USERNAME.value())),
                () -> assertNull(configManagerProvider.get().getCurrentProperty(CliConfigKeys.BASIC_AUTHENTICATION_PASSWORD.value()))
        );
    }

    @Test
    @DisplayName("Should ask for SSL configuration connect to last connected cluster HTTPS url than ask for auth")
    void connectOnStartAskSslAfterAskAuthWrongName() throws IOException {
        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

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
                        + System.lineSeparator())
        );
        // And prompt is disconnected
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        assertAll(() -> assertThat(configManagerProvider.get().getCurrentProperty(CliConfigKeys.REST_TRUST_STORE_PATH.value()))
                        .isEqualTo(escapeWindowsPath(NodeConfig.resolvedTruststorePath)),
                () -> assertThat(configManagerProvider.get().getCurrentProperty(CliConfigKeys.REST_TRUST_STORE_PASSWORD.value()))
                        .isEqualTo(escapeWindowsPath(NodeConfig.trustStorePassword)),

                () -> assertNull(configManagerProvider.get().getCurrentProperty(CliConfigKeys.BASIC_AUTHENTICATION_USERNAME.value())),
                () -> assertNull(configManagerProvider.get().getCurrentProperty(CliConfigKeys.BASIC_AUTHENTICATION_PASSWORD.value()))
        );
    }
}
