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

import static org.apache.ignite.internal.NodeConfig.REST_SSL_BOOTSTRAP_CONFIG;
import static org.apache.ignite.internal.NodeConfig.keyStorePassword;
import static org.apache.ignite.internal.NodeConfig.resolvedKeystorePath;
import static org.apache.ignite.internal.NodeConfig.resolvedTruststorePath;
import static org.apache.ignite.internal.NodeConfig.trustStorePassword;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createClusterUrlSslConfig;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_KEY_STORE_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_KEY_STORE_PATH;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_TRUST_STORE_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_TRUST_STORE_PATH;
import static org.apache.ignite.internal.cli.config.TestStateConfigHelper.createLastConnectedSslDefault;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.escapeWindowsPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.cli.commands.ItConnectToClusterTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ItConnectToSslClusterTest extends ItConnectToClusterTestBase {
    static final String SSL_ERROR_QUESTION = "SSL error occurred while connecting to the node,"
            + " it could be due to the wrong trust store/key store configuration. Do you want to configure them now? [Y/n] ";

    static final String TRUST_STORE_PATH_QUESTION = "Enter trust store path: ";

    static final String TRUST_STORE_PASSWORD_QUESTION = "Enter trust store password: ";

    static final String KEY_STORE_QUESTION = "Do you want to configure key store? [Y/n] ";

    static final String RECONNECT_SSL_QUESTION = "Do you want to reconnect to the last connected node https://localhost:10400? [Y/n] ";

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return REST_SSL_BOOTSTRAP_CONFIG;
    }

    @Test
    @DisplayName("Should connect to last connected cluster HTTPS url")
    void connectOnStart() throws IOException {
        // Given prompt before connect
        assertPromptIs("[disconnected]> ");

        // And default URL is HTTPS
        configManagerProvider.setConfigFile(createClusterUrlSslConfig());
        // And trust store is configured
        setConfigProperty(REST_TRUST_STORE_PATH, resolvedTruststorePath);
        setConfigProperty(REST_TRUST_STORE_PASSWORD, trustStorePassword);

        // And last connected URL is equal to the default URL
        stateConfigProvider.config = createLastConnectedSslDefault();

        // And answer to the first question is "y"
        bindAnswers("y");

        // When asked the question
        question.askQuestionOnReplStart();

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to https://localhost:10400"),
                () -> assertTerminalOutputIs(RECONNECT_SSL_QUESTION),
                () -> assertPromptIs("[" + nodeName() + "]> ")
        );
    }

    @Test
    @DisplayName("Should ask for SSL configuration connect to last connected cluster HTTPS url")
    void connectOnStartAskSsl() throws IOException {
        // Given prompt before connect
        assertPromptIs("[disconnected]> ");

        // And default URL is HTTPS
        configManagerProvider.setConfigFile(createClusterUrlSslConfig());

        // And trust store is not configured

        // And last connected URL is equal to the default URL
        stateConfigProvider.config = createLastConnectedSslDefault();

        // And answer to the reconnect question is "y", to the SSL configuration question is "y",
        // trust store path and password are provided and key store is not configured
        bindAnswers("y", "y", resolvedTruststorePath, trustStorePassword, "n");

        // When asked the question
        question.askQuestionOnReplStart();

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to https://localhost:10400"),
                () -> assertTerminalOutputIs(RECONNECT_SSL_QUESTION
                        + SSL_ERROR_QUESTION + TRUST_STORE_PATH_QUESTION + TRUST_STORE_PASSWORD_QUESTION + KEY_STORE_QUESTION),
                () -> assertPromptIs("[" + nodeName() + "]> "),
                () -> assertThat(getConfigProperty(REST_TRUST_STORE_PATH)).isEqualTo(escapeWindowsPath(resolvedTruststorePath)),
                () -> assertThat(getConfigProperty(REST_TRUST_STORE_PASSWORD)).isEqualTo(escapeWindowsPath(trustStorePassword))
        );
    }

    @Test
    void nodeUrls() {
        // When set up ssl configuration
        setConfigProperty(REST_TRUST_STORE_PATH, resolvedTruststorePath);
        setConfigProperty(REST_TRUST_STORE_PASSWORD, trustStorePassword);
        setConfigProperty(REST_KEY_STORE_PATH, resolvedKeystorePath);
        setConfigProperty(REST_KEY_STORE_PASSWORD, keyStorePassword);

        // And connect via HTTPS
        execute("connect", "https://localhost:10400");

        // Then wait for node names
        await().until(() -> !nodeNameRegistry.names().isEmpty());

        List<String> urls = CLUSTER.runningNodes()
                .map(TestWrappers::unwrapIgniteImpl)
                .map(IgniteImpl::restHttpsAddress)
                .map(address -> "https://" + address)
                .collect(Collectors.toList());

        // Then node urls contain HTTPS urls
        assertThat(nodeNameRegistry.urls()).containsExactlyInAnyOrderElementsOf(urls);
    }
}
