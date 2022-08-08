/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.commands.questions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import jakarta.inject.Inject;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.cli.commands.CliCommandTestInitializedIntegrationBase;
import org.apache.ignite.cli.commands.cliconfig.TestConfigManagerHelper;
import org.apache.ignite.cli.config.ConfigConstants;
import org.apache.ignite.cli.config.TestStateConfigHelper;
import org.apache.ignite.cli.config.TestStateConfigProvider;
import org.apache.ignite.cli.config.ini.IniConfigManager;
import org.apache.ignite.cli.core.flow.question.JlineQuestionWriterReader;
import org.apache.ignite.cli.core.flow.question.QuestionAskerFactory;
import org.apache.ignite.cli.core.repl.prompt.PromptProvider;
import org.jline.reader.impl.LineReaderImpl;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import picocli.CommandLine.Help.Ansi;

class ItConnectToClusterTest extends CliCommandTestInitializedIntegrationBase {
    @Inject
    private PromptProvider promptProvider;

    @Inject
    private TestStateConfigProvider stateConfigProvider;

    @Inject
    private ConnectToClusterQuestion question;

    private Terminal terminal;
    private Path input;

    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);

        input = Files.createTempFile("input", "");
        input.toFile().deleteOnExit();
        terminal = new DumbTerminal(Files.newInputStream(input), new FileOutputStream(FileDescriptor.out));
        LineReaderImpl reader = new LineReaderImpl(terminal);
        QuestionAskerFactory.setReadWriter(new JlineQuestionWriterReader(reader));
    }

    @AfterEach
    public void cleanUp() throws IOException {
        terminal.input().close();
        terminal.close();
    }

    @Test
    @DisplayName("Should connect to last connected cluster url")
    void connectOnStart() throws IOException {
        // Given prompt before connect
        String promptBefore = Ansi.OFF.string(promptProvider.getPrompt());
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
        String promptAfter = Ansi.OFF.string(promptProvider.getPrompt());
        assertThat(promptAfter).isEqualTo("[" + nodeName() + "]> ");
    }

    @Test
    @DisplayName("Should connect to last connected cluster url and ask for save")
    void connectOnStartAndSave() throws IOException {
        // Given prompt before connect
        String promptBefore = Ansi.OFF.string(promptProvider.getPrompt());
        assertThat(promptBefore).isEqualTo("[disconnected]> ");

        // And last connected URL is not equal to the default URL
        configManagerProvider.configManager = new IniConfigManager(TestConfigManagerHelper.createClusterUrlNonDefault());
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
        String promptAfter = Ansi.OFF.string(promptProvider.getPrompt());
        assertThat(promptAfter).isEqualTo("[" + nodeName() + "]> ");
        assertThat(configManagerProvider.get().getCurrentProperty(ConfigConstants.CLUSTER_URL))
                .isEqualTo("http://localhost:10300");
    }

    private String nodeName() {
        return CLUSTER_NODES.get(0).name();
    }

    private void bindAnswers(String... answers) throws IOException {
        Files.writeString(input, String.join("\n", answers) + "\n");
    }
}
