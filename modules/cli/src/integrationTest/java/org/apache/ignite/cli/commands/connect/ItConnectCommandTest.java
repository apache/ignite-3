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

package org.apache.ignite.cli.commands.connect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import jakarta.inject.Inject;
import org.apache.ignite.cli.commands.CliCommandTestIntegrationBase;
import org.apache.ignite.cli.commands.TopLevelCliReplCommand;
import org.apache.ignite.cli.core.repl.prompt.PromptProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import picocli.CommandLine.Help.Ansi;

class ItConnectCommandTest extends CliCommandTestIntegrationBase {
    @Inject
    PromptProvider promptProvider;

    @Override
    protected Class<?> getCommandClass() {
        return TopLevelCliReplCommand.class;
    }

    @Test
    @DisplayName("Should connect to cluster with default url")
    void connectWithDefaultUrl() {
        // Given prompt before connect
        String promptBefore = Ansi.OFF.string(promptProvider.getPrompt());
        assertThat(promptBefore).isEqualTo("[disconnected]> ");

        // When connect without parameters
        execute("connect");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10300")
        );
        // And prompt is changed to connect
        String promptAfter = Ansi.OFF.string(promptProvider.getPrompt());
        assertThat(promptAfter).isEqualTo("[http://localhost:10300]> ");
    }

    @Test
    @DisplayName("Should connect to cluster with given url")
    void connectWithGivenUrl() {
        // When connect with given url
        execute("connect", "http://localhost:10301");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10301")
        );
    }

    @Test
    @DisplayName("Should not connect to cluster with wrong url")
    void connectWithWrongUrl() {
        // When connect with wrong url
        execute("connect", "http://localhost:11111");

        // Then
        assertAll(
                () -> assertErrOutputIs("Could not connect to URL: http://localhost:11111" + System.lineSeparator())
        );
        // And prompt is
        String prompt = Ansi.OFF.string(promptProvider.getPrompt());
        assertThat(prompt).isEqualTo("[disconnected]> ");
    }

    @Test
    @DisplayName("Should disconnect after connect")
    void disconnect() {
        // Given connected to cluster
        execute("connect");
        // And prompt is
        String promptBefore = Ansi.OFF.string(promptProvider.getPrompt());
        assertThat(promptBefore).isEqualTo("[http://localhost:10300]> ");

        // When disconnect
        execute("disconnect");
        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Disconnected from http://localhost:10300")
        );
        // And prompt is changed
        String promptAfter = Ansi.OFF.string(promptProvider.getPrompt());
        assertThat(promptAfter).isEqualTo("[disconnected]> ");
    }
}
