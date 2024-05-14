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
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests for ignite node commands with a provided node name. */
public class ItNodeNameTest extends CliIntegrationTest {

    @Override
    protected Class<?> getCommandClass() {
        return TopLevelCliReplCommand.class;
    }

    @BeforeEach
    void connect() {
        execute("connect");
        resetOutput();
        // wait to pulling node names
        await().until(() -> !nodeNameRegistry.names().isEmpty());
    }

    @Test
    void nodeUrls() {
        List<String> urls = CLUSTER.runningNodes()
                .map(IgniteImpl::restHttpAddress)
                .map(address -> "http://" + address)
                .collect(Collectors.toList());

        // Node urls contain HTTP urls
        assertThat(nodeNameRegistry.urls()).containsExactlyInAnyOrderElementsOf(urls);
    }

    @Test
    @DisplayName("Should display node version with provided node name")
    void nodeVersion() {
        // When
        execute("node", "version", "--node", nodeName());

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputMatches("[1-9]\\d*\\.\\d+\\.\\d+(?:-[a-zA-Z0-9]+)?\\s+")
        );
    }

    @Test
    @DisplayName("Should display node config with provided node name")
    void nodeConfig() {
        // When
        execute("node", "config", "show", "--node", nodeName());

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );
    }

    @Test
    @DisplayName("Should display node status with provided node name")
    void nodeStatus() {
        // When
        String nodeName = nodeName();
        execute("node", "status", "--node", nodeName);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputMatches("\\[name: " + nodeName + ", state: started\\]?\\s+")
        );
    }

    private String nodeName() {
        return nodeNameRegistry.names()
                .stream()
                .findAny()
                .orElseThrow(() -> new IllegalStateException("nodeNameRegistry doesn't have any node names"));
    }
}
