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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.IOException;
import org.apache.ignite.internal.cli.commands.ItConnectToClusterTestBase;
import org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.TestStateConfigHelper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ItConnectToClusterTest extends ItConnectToClusterTestBase {

    @Test
    @DisplayName("Should connect to last connected cluster url on start")
    void connectOnStart() throws IOException {
        // Given prompt before connect
        assertPromptIs("[disconnected]> ");

        // And last connected URL is equal to the default URL
        stateConfigProvider.config = TestStateConfigHelper.createLastConnectedDefault();

        // And answer to the first question is "y"
        bindAnswers("y");

        // When asked the question
        question.askQuestionOnReplStart();

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10300"),
                () -> assertTerminalOutputIs(RECONNECT_QUESTION),
                () -> assertPromptIs("[" + nodeName() + "]> ")
        );
    }

    @Test
    @DisplayName("Executes a command without connect using default cluster url")
    void executeWithDefaultUrl() throws IOException {
        // Given prompt before connect
        assertPromptIs("[disconnected]> ");

        // And answer to the first question is "y"
        bindAnswers("y");

        // When asked the question
        execute("cluster", "status");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs("[name: cluster, nodes: 3, status: active, cmgNodes: [ictct_n_3344], msNodes: [ictct_n_3344]]"
                        + System.lineSeparator()),
                this::assertTerminalOutputIsEmpty,
                () -> assertPromptIs("[disconnected]> ")
        );
    }

    @Test
    @DisplayName("Should connect to last connected cluster url and ask for save")
    void connectOnStartAndSave() throws IOException {
        // Given prompt before connect
        assertPromptIs("[disconnected]> ");

        // And last connected URL is not equal to the default URL
        configManagerProvider.setConfigFile(TestConfigManagerHelper.createClusterUrlNonDefaultConfig());
        stateConfigProvider.config = TestStateConfigHelper.createLastConnectedDefault();

        // And answer to both questions is "y"
        bindAnswers("y", "y");

        // When asked the questions
        question.askQuestionOnReplStart();

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10300"),
                () -> assertOutputContains("Config saved"),
                () -> assertTerminalOutputIs(RECONNECT_QUESTION
                        + "Would you like to use http://localhost:10300 as the default URL? [Y/n] "),
                () -> assertPromptIs("[" + nodeName() + "]> "),
                () -> assertThat(getConfigProperty(CliConfigKeys.CLUSTER_URL)).isEqualTo("http://localhost:10300")
        );
    }

    @Test
    @DisplayName("Should not ask to connect to different URL when disconnected")
    void connectToAnotherUrl() {
        // Given prompt before connect
        assertPromptIs("[disconnected]> ");

        // And connected
        execute("connect");

        // And output is
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs("Connected to http://localhost:10300" + System.lineSeparator()),
                this::assertTerminalOutputIsEmpty
        );

        // And disconnect
        execute("disconnect");

        // When connect to different URL
        execute("connect", "http://localhost:10301");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs("Connected to http://localhost:10301" + System.lineSeparator()),
                this::assertTerminalOutputIsEmpty,
                () -> assertPromptIs("[" + CLUSTER.node(1).name() + "]> ")
        );
    }

    @Test
    @DisplayName("Should ask to connect to different URL when connected")
    void connectToAnotherUrlWhenConnected() throws IOException {
        // Given prompt before connect
        assertPromptIs("[disconnected]> ");

        // And connected
        execute("connect");

        // And output is
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs("Connected to http://localhost:10300" + System.lineSeparator()),
                this::assertTerminalOutputIsEmpty
        );

        resetTerminalOutput();

        // And answer is "y"
        bindAnswers("y");

        // When connect to different URL
        execute("connect", "http://localhost:10301");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs("Connected to http://localhost:10301" + System.lineSeparator()),
                () -> assertTerminalOutputIs("You are already connected to the http://localhost:10300,"
                        + " do you want to connect to the http://localhost:10301? [Y/n] "),
                () -> assertPromptIs("[" + CLUSTER.node(1).name() + "]> ")
        );
    }
}
