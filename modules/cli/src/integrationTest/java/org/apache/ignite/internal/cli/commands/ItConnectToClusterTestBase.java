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

package org.apache.ignite.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.internal.cli.config.TestStateConfigProvider;
import org.apache.ignite.internal.cli.core.flow.question.JlineQuestionWriterReaderFactory;
import org.apache.ignite.internal.cli.core.flow.question.QuestionAskerFactory;
import org.apache.ignite.internal.cli.core.repl.prompt.PromptProvider;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import picocli.CommandLine.Help.Ansi;

/**
 * Base class for testing 'connect' command.
 */
public class ItConnectToClusterTestBase extends CliIntegrationTest {
    protected static final String AUTH_ERROR_QUESTION = "Authentication error occurred while connecting to the node,"
            + " it could be due to the wrong basic auth configuration. Do you want to configure them now? [Y/n] ";
    protected static final String REMEMBER_CREDENTIALS_QUESTION = "Remember current credentials? [Y/n] ";

    protected static final String USERNAME_QUESTION = "Enter username: ";

    protected static final String PASSWORD_QUESTION = "Enter user password: ";

    protected static final String RECONNECT_QUESTION = "Do you want to reconnect to the last connected node http://localhost:10300? [Y/n] ";

    @Inject
    protected TestStateConfigProvider stateConfigProvider;

    @Inject
    protected ConnectToClusterQuestion question;

    @Inject
    private PromptProvider promptProvider;

    private Terminal terminal;

    private Path input;

    private ByteArrayOutputStream output;

    @Override
    protected Class<?> getCommandClass() {
        return TopLevelCliReplCommand.class;
    }

    @BeforeEach
    public void setUpTerminal() throws Exception {
        input = Files.createTempFile(WORK_DIR, "input", "");
        output = new ByteArrayOutputStream();
        terminal = new DumbTerminal(Files.newInputStream(input), output);
        QuestionAskerFactory.setWriterReaderFactory(new JlineQuestionWriterReaderFactory(terminal));
    }

    @AfterEach
    public void cleanUp() throws IOException {
        terminal.input().close();
        terminal.close();
    }

    protected String getPrompt() {
        return Ansi.OFF.string(promptProvider.getPrompt());
    }

    protected static String nodeName() {
        return CLUSTER.node(0).name();
    }

    protected void bindAnswers(String... answers) throws IOException {
        Files.writeString(input, String.join("\n", answers) + "\n");
    }

    protected void resetTerminalOutput() {
        output.reset();
    }

    protected void assertTerminalOutputIsEmpty() {
        assertThat(output.toString())
                .as("Expected terminal output to be empty")
                .isEmpty();
    }

    protected void assertTerminalOutputIs(String expectedTerminalOutput) {
        assertThat(output.toString())
                .as("Expected terminal output to be equal to:")
                .isEqualTo(expectedTerminalOutput);
    }

    protected void assertPromptIs(String expectedPromptOutput) {
        assertThat(getPrompt())
                .as("Expected prompt to be equal to:")
                .isEqualTo(expectedPromptOutput);
    }
}
