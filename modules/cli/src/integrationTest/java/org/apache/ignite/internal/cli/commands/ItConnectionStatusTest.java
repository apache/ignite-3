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
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import jakarta.inject.Inject;
import org.apache.ignite.internal.cli.core.repl.Session;
import org.apache.ignite.internal.cli.core.repl.SessionInfo.ConnectionStatus;
import org.apache.ignite.internal.cli.core.repl.prompt.PromptProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import picocli.CommandLine.Help.Ansi;

class ItConnectionStatusTest extends CliCommandTestInitializedIntegrationBase {
    @Inject
    PromptProvider promptProvider;

    @Inject
    Session session;

    @Override
    protected Class<?> getCommandClass() {
        return TopLevelCliReplCommand.class;
    }

    @Test
    @DisplayName("Should connect to cluster with default url")
    void connectWithDefaultUrl() throws NoSuchFieldException, IllegalAccessException {
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

        // And prompt is changed to green (connection status OPEN)
        assertEquals(ConnectionStatus.OPEN, session.info().connectionStatus());
    }

    private String nodeName() {
        return CLUSTER_NODES.get(0).name();
    }
}
