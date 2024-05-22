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
    @DisplayName("Should connect to last connected cluster url")
    void connectOnStart() throws IOException {
        // Given prompt before connect
        String promptBefore = getPrompt();
        assertThat(promptBefore).isEqualTo("[disconnected]> ");

        // And last connected URL is equal to the default URL
        stateConfigProvider.config = TestStateConfigHelper.createLastConnectedDefault();

        // And answer to the first question is "y"
        bindAnswers("y");

        // When asked the question
        question.askQuestionOnReplStart();

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10300")
        );
        // And prompt is changed to connect
        String promptAfter = getPrompt();
        assertThat(promptAfter).isEqualTo("[" + nodeName() + "]> ");
    }

    @Test
    @DisplayName("Should connect to last connected cluster url and ask for save")
    void connectOnStartAndSave() throws IOException {
        // Given prompt before connect
        String promptBefore = getPrompt();
        assertThat(promptBefore).isEqualTo("[disconnected]> ");

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
                () -> assertOutputContains("Config saved")
        );
        // And prompt is changed to connect
        String promptAfter = getPrompt();
        assertThat(promptAfter).isEqualTo("[" + nodeName() + "]> ");
        assertThat(getConfigProperty(CliConfigKeys.CLUSTER_URL)).isEqualTo("http://localhost:10300");
    }

    @Test
    @DisplayName("Should ask to connect to different URL")
    void connectToAnotherUrl() throws IOException {
        // Given prompt before connect
        String promptBefore = getPrompt();
        assertThat(promptBefore).isEqualTo("[disconnected]> ");

        // And connected
        execute("connect");

        // And output is
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs("Connected to http://localhost:10300" + System.lineSeparator())
        );

        // And answer is "y"
        bindAnswers("y");

        // And disconnect
        execute("disconnect");

        // When connect to different URL
        execute("connect", "http://localhost:10301");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs("Connected to http://localhost:10301" + System.lineSeparator())
        );
        // And prompt is changed to another node
        String promptAfter = getPrompt();
        assertThat(promptAfter).isEqualTo("[" + CLUSTER.node(1).name() + "]> ");
    }

}
