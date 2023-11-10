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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.escapeWindowsPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.IOException;
import org.apache.ignite.internal.NodeConfig;
import org.apache.ignite.internal.cli.commands.ItConnectToClusterTestBase;
import org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.TestStateConfigHelper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ItConnectToSslClusterTest extends ItConnectToClusterTestBase {
    @Override
    protected String nodeBootstrapConfigTemplate() {
        return NodeConfig.REST_SSL_BOOTSTRAP_CONFIG;
    }

    @Test
    @DisplayName("Should connect to last connected cluster HTTPS url")
    void connectOnStart() throws IOException {
        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // And default URL is HTTPS
        configManagerProvider.setConfigFile(TestConfigManagerHelper.createClusterUrlSslConfig());
        // And trust store is configured
        configManagerProvider.configManager.setProperty(CliConfigKeys.REST_TRUST_STORE_PATH.value(), NodeConfig.resolvedTruststorePath);
        configManagerProvider.configManager.setProperty(CliConfigKeys.REST_TRUST_STORE_PASSWORD.value(), NodeConfig.trustStorePassword);

        // And last connected URL is equal to the default URL
        stateConfigProvider.config = TestStateConfigHelper.createLastConnectedSslDefault();

        // And answer to the first question is "y"
        bindAnswers("y");

        // When asked the question
        question.askQuestionOnReplStart();

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to https://localhost:10400")
        );
        // And prompt is changed to connect
        assertThat(getPrompt()).isEqualTo("[" + nodeName() + "]> ");
    }

    @Test
    @DisplayName("Should ask for SSL configuration connect to last connected cluster HTTPS url")
    void connectOnStartAskSsl() throws IOException {
        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // And default URL is HTTPS
        configManagerProvider.setConfigFile(TestConfigManagerHelper.createClusterUrlSslConfig());

        // And trust store is not configured

        // And last connected URL is equal to the default URL
        stateConfigProvider.config = TestStateConfigHelper.createLastConnectedSslDefault();

        // And answer to the reconnect question is "y", to the SSL configuration question is "y",
        // trust store path and password are provided and key store is not configured
        bindAnswers("y", "y", NodeConfig.resolvedTruststorePath, NodeConfig.trustStorePassword, "n");

        // When asked the question
        question.askQuestionOnReplStart();

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to https://localhost:10400")
        );
        // And prompt is changed to connect
        assertThat(getPrompt()).isEqualTo("[" + nodeName() + "]> ");

        assertThat(configManagerProvider.get().getCurrentProperty(CliConfigKeys.REST_TRUST_STORE_PATH.value()))
                .isEqualTo(escapeWindowsPath(NodeConfig.resolvedTruststorePath));
        assertThat(configManagerProvider.get().getCurrentProperty(CliConfigKeys.REST_TRUST_STORE_PASSWORD.value()))
                .isEqualTo(escapeWindowsPath(NodeConfig.trustStorePassword));
    }
}
