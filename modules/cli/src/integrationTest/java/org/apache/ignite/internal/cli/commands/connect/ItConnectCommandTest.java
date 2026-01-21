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

import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createEmptySecretConfig;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createIntegrationTestsConfig;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createJdbcTestsBasicSecretConfig;
import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.createNonExistingSecretConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.File;
import java.util.List;
import org.apache.ignite.internal.cli.commands.ItConnectToClusterTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
                () -> assertOutputContains("Connected to http://localhost:10300"),
                this::assertTerminalOutputIsEmpty
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
                () -> assertOutputContains("Connected to http://localhost:10301"),
                this::assertTerminalOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Should connect to cluster with given profile")
    void connectWithGivenProfile() {
        // When connect with given url
        execute("connect", "--profile", "test");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10301"),
                this::assertTerminalOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Should connect to cluster with given url with trailing slash")
    void connectWithGivenUrlWithTrailingSlash() {
        // When connect with given url with trailing slash
        execute("connect", "http://localhost:10301/");

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10301"),
                this::assertTerminalOutputIsEmpty
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
                        + "Could not connect to node with URL http://localhost:11111" + System.lineSeparator()),
                this::assertTerminalOutputIsEmpty
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
                () -> assertOutputContains("Disconnected from http://localhost:10300"),
                this::assertTerminalOutputIsEmpty
        );
        // And prompt is changed
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");
    }

    private static List<Arguments> secretConfigs() {
        return List.of(
                Arguments.of(Named.of("createEmptySecretConfig", createEmptySecretConfig())),
                Arguments.of(Named.of("createNonExistingSecretConfig", createNonExistingSecretConfig())),
                Arguments.of(Named.of("createJdbcTestsBasicSecretConfig", createJdbcTestsBasicSecretConfig()))
        );
    }

    @ParameterizedTest
    @MethodSource("secretConfigs")
    @DisplayName("Should state that already connected")
    void connectTwice(File secretConfig) {
        // Given secret config
        configManagerProvider.setConfigFile(createIntegrationTestsConfig(), secretConfig);

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
                () -> assertOutputIs("You are already connected to http://localhost:10300" + System.lineSeparator()),
                this::assertTerminalOutputIsEmpty
        );
        // And prompt is still connected
        assertThat(getPrompt()).isEqualTo("[" + nodeName() + "]> ");
    }

    @Test
    @DisplayName("Should throw error if cluster without authentication but command invoked with username/password")
    void clusterWithoutAuthButUsernamePasswordProvided() {

        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // When connect with auth parameters
        execute("connect", "--username", "admin", "--password", "password");

        // Then
        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputIs("Authentication is not enabled on cluster but username or password were provided."
                        + System.lineSeparator()),
                this::assertTerminalOutputIsEmpty
        );

        // And prompt is
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");
    }
}
