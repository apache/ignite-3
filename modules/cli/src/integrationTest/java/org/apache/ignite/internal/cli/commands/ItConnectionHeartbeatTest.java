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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.verify;

import jakarta.inject.Inject;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.cli.core.repl.ConnectionHeartBeat;
import org.apache.ignite.internal.cli.core.repl.Session;
import org.apache.ignite.internal.cli.core.repl.prompt.ReplPromptProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ItConnectionHeartbeatTest extends CliCommandTestInitializedIntegrationBase {

    @Inject
    Session session;

    @Spy
    @Inject
    private ReplPromptProvider replPromptProvider;

    @BeforeEach
    void setUp() {
        //Set connection check timeout to 1 sec to make test fast
        ConnectionHeartBeat.CLI_CHECK_CONNECTION_PERIOD_SECONDS = 1L;
    }

    @Override
    protected Class<?> getCommandClass() {
        return TopLevelCliReplCommand.class;
    }

    @Test
    @DisplayName("Should invoke onConnection() on connection established")
    void connectionEstablished() {
        // Given null session info before connect
        assertNull(session.info());

        // When connect without parameters
        execute("connect");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10300")
        );

        verify(replPromptProvider, after(ConnectionHeartBeat.CLI_CHECK_CONNECTION_PERIOD_SECONDS * 2L).times(1)).onConnection();
        verify(replPromptProvider, after(ConnectionHeartBeat.CLI_CHECK_CONNECTION_PERIOD_SECONDS * 2L).never()).onConnectionLost();
    }

    @Test
    @DisplayName("Should invoke onConnectionLost()")
    void onConnectionLost() {
        // Given connected cli
        execute("connect");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10300")
        );

        // When stop all nodes
        allNodeNames().forEach(IgnitionManager::stop);

        verify(replPromptProvider, after(ConnectionHeartBeat.CLI_CHECK_CONNECTION_PERIOD_SECONDS * 2L).times(1)).onConnectionLost();
        verify(replPromptProvider, after(ConnectionHeartBeat.CLI_CHECK_CONNECTION_PERIOD_SECONDS * 2L).times(1)).onConnection();
    }
}
