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

package org.apache.ignite.internal.cli.commands.connect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.IOException;
import org.apache.ignite.internal.cli.commands.ItConnectToClusterTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ItConnectCommandTest extends ItConnectToClusterTestBase {

    @Test
    @DisplayName("Should connect to cluster with default url")
    void connectWithDefaultUrl() {
        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // When connect without parameters
        execute("connect");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10300")
        );
        // And prompt is changed to connect
        assertThat(getPrompt()).isEqualTo("[" + nodeName() + "]> ");
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
                () -> assertErrOutputIs("Node unavailable" + System.lineSeparator()
                        + "Could not connect to node with URL http://localhost:11111" + System.lineSeparator())
        );
        // And prompt is
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");
    }

    @Test
    @DisplayName("Should disconnect after connect")
    void disconnect() {
        // Given connected to cluster
        execute("connect");
        // And prompt is
        assertThat(getPrompt()).isEqualTo("[" + nodeName() + "]> ");

        // When disconnect
        execute("disconnect");
        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Disconnected from http://localhost:10300")
        );
        // And prompt is changed
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");
    }

    @Test
    @DisplayName("Should state that already connected")
    void connectTwice() {
        // Given connected to cluster
        execute("connect");
        // And output is
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs("Connected to http://localhost:10300" + System.lineSeparator())
        );
        // And prompt is
        assertThat(getPrompt()).isEqualTo("[" + nodeName() + "]> ");

        // When connect again
        execute("connect");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs("You are already connected to http://localhost:10300" + System.lineSeparator())
        );
        // And prompt is still connected
        assertThat(getPrompt()).isEqualTo("[" + nodeName() + "]> ");
    }

    @Test
    @DisplayName("Should throw error if cluster without authentication but command invoked with username/password")
    void clusterWithoutAuthButUsernamePasswordProvided() throws IOException {

        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // When connect with auth parameters
        execute("connect", "--username", "admin", "--password", "password");

        // Then
        assertAll(
                () -> assertErrOutputIs("Authentication is not enabled on cluster but username or password were provided."
                        + System.lineSeparator())
        );

        // And prompt is
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");
    }
}
