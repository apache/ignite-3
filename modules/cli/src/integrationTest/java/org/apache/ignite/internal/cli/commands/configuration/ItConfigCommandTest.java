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

package org.apache.ignite.internal.cli.commands.configuration;


import static org.apache.ignite.internal.cli.core.style.AnsiStringSupport.fg;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.apache.ignite.internal.cli.core.style.AnsiStringSupport.Color;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for cluster/node config commands.
 */
class ItConfigCommandTest extends CliIntegrationTest {
    @Test
    @DisplayName("Should read config when valid cluster-endpoint-url is given")
    void readDefaultConfig() {
        // When read cluster config with valid url
        execute("cluster", "config", "show", "--cluster-endpoint-url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );
    }

    @Test
    @DisplayName("Should update config with hocon format when valid cluster-endpoint-url is given")
    void addConfigKeyValue() {
        // When update default data storage to rocksdb
        execute("cluster", "config", "update", "--cluster-endpoint-url", NODE_URL, "{metaStorage: {idleSyncTimeInterval: 1000}}");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );

        // When read the updated cluster configuration
        execute("cluster", "config", "show", "--cluster-endpoint-url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("\"idleSyncTimeInterval\" : 1000")
        );
    }

    @Test
    @DisplayName("Should update config with hocon format when valid cluster-endpoint-url is given")
    void addNodeConfigKeyValue() {
        // When update default data storage to rocksdb
        execute("node", "config", "update", "--node-url", NODE_URL,
                "network.nodeFinder.netClusterNodes : [ \"localhost:3344\", \"localhost:3345\" ]");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Node configuration updated. "
                        + fg(Color.YELLOW).mark("Restart the node to apply changes."))
        );

        // When read the updated cluster configuration
        execute("node", "config", "show", "--node-url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("\"netClusterNodes\" : [ \"localhost:3344\", \"localhost:3345\" ]")
        );
    }

    @Test
    @DisplayName("Should update config with key-value format when valid cluster-endpoint-url is given")
    void updateConfigWithSpecifiedPath() {
        // When update default data storage to rocksdb
        execute("cluster", "config", "update", "--cluster-endpoint-url", NODE_URL, "metaStorage.idleSyncTimeInterval=2000");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );

        // When read the updated cluster configuration
        execute("cluster", "config", "show", "--cluster-endpoint-url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("\"idleSyncTimeInterval\" : 2000")
        );
    }

    @Test
    @DisplayName("Should update config with key-value format when valid cluster-endpoint-url is given")
    void updateClusterConfigWithoutQuoting() {
        execute("cluster", "config", "update", "--cluster-endpoint-url", NODE_URL,
                "security.authentication.providers.default={type=basic,users=[{username=asd,password=pass1}]}");

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );


        // Emulate config with spaces
        execute("cluster", "config", "update", "--cluster-endpoint-url", NODE_URL,
                "security.authentication.providers.default", "=", "{", "type=basic,", "users=[{", "username=asd,", "password=pass2}]}");

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );
    }

    @Test
    @DisplayName("Test different types of quoted parameters")
    void updateClusterWithQuotedArgs() {
        // Emulate quoting config
        execute("cluster", "config", "update", "--cluster-endpoint-url", NODE_URL,
                "\"security.authentication.providers.default={type=basic,users=[{username=asd,password=pass3}]}\"");

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );

        // Emulate quoting config
        execute("cluster", "config", "update", "--cluster-endpoint-url", NODE_URL,
                "\"security.authentication.providers.default\"", "\"={type=basic,users=[{username=asd,password=pass4}]}\"");

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );

        // Emulate quoting config
        execute("cluster", "config", "update", "--cluster-endpoint-url", NODE_URL,
                "security.authentication.providers.default", "\"={type=basic,users=[{username=asd,password=pass5}]}\"");

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );
    }

    @Test
    @DisplayName("Test using arguments in parameters")
    void useOptionsInArguments() {
        execute("cluster", "config", "update", "--cluster-endpoint-url", NODE_URL,
                "security.authentication.providers.default={type=basic,users=[{username:", "--verbose,", "password=--verbose}]}");

        assertAll(
                () -> assertExitCodeIs(2),
                () -> assertErrOutputContains("Unknown option: '--verbose,'"),
                this::assertOutputIsEmpty
        );

        resetOutput();

        execute("cluster", "config", "update", "--cluster-endpoint-url", NODE_URL,
                "\"security.authentication.providers.default={type=basic,users=[{username: --verbose, password=--verbose}]}\"");

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );
    }

    @Test
    void updateWithWrongData() {
        execute("node", "config", "update", "--node-url", NODE_URL, "network.foo=\"bar\"");

        assertAll(
                () -> assertExitCodeIs(1),
                () -> assertErrOutputContains("'network' configuration doesn't have the 'foo' sub-configuration"),
                this::assertOutputIsEmpty
        );

        resetOutput();

        execute("node", "config", "update", "--node-url", NODE_URL, "network.shutdownQuietPeriod=asd");

        assertAll(
                () -> assertExitCodeIs(1),
                () -> assertErrOutputContains("'long' is expected as a type for the 'network.shutdownQuietPeriod' configuration value"),
                this::assertOutputIsEmpty
        );
    }

    @Test
    public void partialGet() {
        execute("node", "config", "show", "--node-url", NODE_URL, "network");
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("\"inbound\""),
                () -> assertOutputDoesNotContain("\"node\"")
        );
    }
}
