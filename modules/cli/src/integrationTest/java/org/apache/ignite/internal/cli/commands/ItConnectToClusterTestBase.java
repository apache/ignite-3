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

import jakarta.inject.Inject;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
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
    @Inject
    protected TestStateConfigProvider stateConfigProvider;
    @Inject
    protected ConnectToClusterQuestion question;

    @Inject
    private PromptProvider promptProvider;

    private Terminal terminal;

    private Path input;

    @Override
    protected Class<?> getCommandClass() {
        return TopLevelCliReplCommand.class;
    }

    @BeforeEach
    public void setUpTerminal() throws Exception {
        input = Files.createTempFile(WORK_DIR, "input", "");
        terminal = new DumbTerminal(Files.newInputStream(input), new FileOutputStream(FileDescriptor.out));
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

    protected String nodeName() {
        return CLUSTER.node(0).name();
    }

    protected void bindAnswers(String... answers) throws IOException {
        Files.writeString(input, String.join("\n", answers) + "\n");
    }
}
