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

import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.internal.cli.CliIntegrationTest;
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
        execute("cluster", "config", "show", "--url", NODE_URL);

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
        execute("cluster", "config", "update", "--url", NODE_URL, "{ignite{system: {idleSafeTimeSyncIntervalMillis: 1000}}}");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );

        // When read the updated cluster configuration
        execute("cluster", "config", "show", "--url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("idleSafeTimeSyncIntervalMillis=1000")
        );
    }

    @Test
    void addConfigFromFile() {
        execute("cluster", "config", "show", "--url", NODE_URL);

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("dataAvailabilityTimeMillis=600000"),
                () -> assertOutputContains("batchSize=5")
        );

        execute("cluster", "config", "update", "--url", NODE_URL, "--file", getClass().getResource("/ignite-cluster.conf").getPath());

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );

        execute("cluster", "config", "show", "--url", NODE_URL);

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("dataAvailabilityTimeMillis=600001"),
                () -> assertOutputContains("batchSize=1")
        );

        // Set to default value
        execute("cluster", "config", "update", "--url", NODE_URL, "ignite.gc.lowWatermark.dataAvailabilityTimeMillis=600000");
        execute("cluster", "config", "update", "--url", NODE_URL, "ignite.gc.batchSize=5");

        execute("cluster", "config", "show", "--url", NODE_URL);

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("dataAvailabilityTimeMillis=600000"),
                () -> assertOutputContains("batchSize=5")
        );
    }

    @Test
    void addConfigFromFileAndMergeFromArgs() {
        execute("cluster", "config", "show", "--url", NODE_URL);

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("dataAvailabilityTimeMillis=600000"),
                () -> assertOutputContains("batchSize=5")
        );

        execute("cluster", "config", "update", "--url", NODE_URL,
                "--file",
                getClass().getResource("/ignite-cluster.conf").getPath(),
                "ignite.gc.lowWatermark.dataAvailabilityTimeMillis=600002"
        );

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );

        execute("cluster", "config", "show", "--url", NODE_URL);

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("dataAvailabilityTimeMillis=600002"),
                () -> assertOutputContains("batchSize=1")
        );

        execute("cluster", "config", "update", "--url", NODE_URL, "ignite.gc.lowWatermark.dataAvailabilityTimeMillis=600000");
        execute("cluster", "config", "update", "--url", NODE_URL, "ignite.gc.batchSize=5");

        execute("cluster", "config", "show", "--url", NODE_URL);

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("dataAvailabilityTimeMillis=600000"),
                () -> assertOutputContains("batchSize=5")
        );
    }

    @Test
    void addConfigFromNonExistingFile() {
        execute("cluster", "config", "update", "--url", NODE_URL, "--file", "wrongPath");

        assertAll(
                this::assertExitCodeIsError,
                () -> assertErrOutputContains("File ["),
                () -> assertErrOutputContains("] not found"),
                this::assertOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Should update config with hocon format when valid cluster-endpoint-url is given")
    void addNodeConfigKeyValue() {
        // When update default data storage to rocksdb
        execute("node", "config", "update", "--url", NODE_URL,
                "ignite.network.nodeFinder.netClusterNodes : [ \"localhost:3344\", \"localhost:3345\" ]");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Node configuration updated. Restart the node to apply changes.")
        );

        // When read the updated cluster configuration
        execute("node", "config", "show", "--url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("netClusterNodes=[\n"),
                () -> assertOutputContains("\"localhost:3344\",\n"),
                () -> assertOutputContains("\"localhost:3345\"\n")
        );
    }

    @Test
    void addNodeConfigFromFile() {
        execute("node", "config", "show", "--url", NODE_URL);

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("queueMaxSize=2147483647"),
                () -> assertOutputContains("statesLifetimeMillis=60000")
        );

        execute("node", "config", "update", "--url", NODE_URL, "--file", getClass().getResource("/ignite-node.conf").getPath());

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Node configuration updated. Restart the node to apply changes.")
        );

        execute("node", "config", "show", "--url", NODE_URL);

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("queueMaxSize=1000"),
                () -> assertOutputContains("statesLifetimeMillis=1001")
        );

        // Set default values
        execute("node", "config", "update", "--url", NODE_URL, "ignite.compute.queueMaxSize=2147483647");
        execute("node", "config", "update", "--url", NODE_URL, "ignite.compute.statesLifetimeMillis=60000");

        execute("node", "config", "show", "--url", NODE_URL);

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("queueMaxSize=2147483647"),
                () -> assertOutputContains("statesLifetimeMillis=60000")
        );
    }

    @Test
    void addNodeConfigFromFileAndMergeFromFiles() {
        execute("node", "config", "show", "--url", NODE_URL);

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("queueMaxSize=2147483647"),
                () -> assertOutputContains("statesLifetimeMillis=60000")
        );

        execute("node", "config", "update", "--url", NODE_URL,
                "--file", getClass().getResource("/ignite-node.conf").getPath(), "ignite.compute.statesLifetimeMillis=1337");

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Node configuration updated. Restart the node to apply changes.")
        );

        execute("node", "config", "show", "--url", NODE_URL);

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("queueMaxSize=1000"),
                () -> assertOutputContains("statesLifetimeMillis=1337")
        );

        // Set default values
        execute("node", "config", "update", "--url", NODE_URL, "ignite.compute.queueMaxSize=2147483647");
        execute("node", "config", "update", "--url", NODE_URL, "ignite.compute.statesLifetimeMillis=60000");

        execute("node", "config", "show", "--url", NODE_URL);

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("queueMaxSize=2147483647"),
                () -> assertOutputContains("statesLifetimeMillis=60000")
        );
    }

    @Test
    void addNodeConfigFromNonExistingFile() {
        execute("node", "config", "update", "--url", NODE_URL, "--file", "wrongPath");

        assertAll(
                this::assertExitCodeIsError,
                () -> assertErrOutputContains("File ["),
                () -> assertErrOutputContains("] not found"),
                this::assertOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Should update config with key-value format when valid cluster-endpoint-url is given")
    void updateConfigWithSpecifiedPath() {
        // When update default data storage to rocksdb
        execute("cluster", "config", "update", "--url", NODE_URL, "ignite.system.idleSafeTimeSyncIntervalMillis=2000");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );

        // When read the updated cluster configuration
        execute("cluster", "config", "show", "--url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("idleSafeTimeSyncIntervalMillis=2000")
        );
    }

    @Test
    @DisplayName("Should update config with key-value format when valid cluster-endpoint-url is given")
    void updateClusterConfigWithoutQuoting() {
        execute("cluster", "config", "update", "--url", NODE_URL,
                "ignite.security.authentication.providers.default={type=basic,users=[{username=asd,password=pass1}]}");

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );

        // Emulate config with spaces
        execute("cluster", "config", "update", "--url", NODE_URL,
                "ignite.security.authentication.providers.default", "=",
                "{", "type=basic,", "users=[{", "username=asd,", "password=pass2}]}");

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
        execute("cluster", "config", "update", "--url", NODE_URL,
                "\"ignite.security.authentication.providers.default={type=basic,users=[{username=asd,password=pass3}]}\"");

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );

        // Emulate quoting config
        execute("cluster", "config", "update", "--url", NODE_URL,
                "\"ignite.security.authentication.providers.default\"", "\"={type=basic,users=[{username=asd,password=pass4}]}\"");

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );

        // Emulate quoting config
        execute("cluster", "config", "update", "--url", NODE_URL,
                "ignite.security.authentication.providers.default", "\"={type=basic,users=[{username=asd,password=pass5}]}\"");

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );
    }

    @Test
    @DisplayName("Test using arguments in parameters")
    void useOptionsInArguments() {
        execute("cluster", "config", "update", "--url", NODE_URL,
                "ignite.security.authentication.providers.default={type=basic,users=[{username:", "--verbose,", "password=--verbose}]}");

        assertAll(
                () -> assertExitCodeIs(2),
                () -> assertErrOutputContains("Unknown option: '--verbose,'"),
                this::assertOutputIsEmpty
        );

        execute("cluster", "config", "update", "--url", NODE_URL,
                "\"ignite.security.authentication.providers.default={type=basic,users=[{username: --verbose, password=--verbose}]}\"");

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );
    }

    @Test
    void updateWithWrongData() {
        execute("node", "config", "update", "--url", NODE_URL, "ignite.network.foo=\"bar\"");

        assertAll(
                this::assertExitCodeIsError,
                () -> assertErrOutputContains("'ignite.network' configuration doesn't have the 'foo' sub-configuration"),
                this::assertOutputIsEmpty
        );

        execute("node", "config", "update", "--url", NODE_URL, "ignite.network.shutdownQuietPeriodMillis=asd");

        assertAll(
                this::assertExitCodeIsError,
                () -> assertErrOutputContains("'long' is expected as a type for the "
                        + "'ignite.network.shutdownQuietPeriodMillis' configuration value"),
                this::assertOutputIsEmpty
        );
    }

    @Test
    public void partialGet() {
        execute("node", "config", "show", "--url", NODE_URL);
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("inbound {"),
                () -> assertOutputContains("clientConnector {")
        );

        execute("node", "config", "show", "--url", NODE_URL, "ignite.network");
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("inbound {"),
                () -> assertOutputDoesNotContain("clientConnector {")
        );
    }

    @Test
    public void getList() {
        execute("node", "config", "show", "--url", NODE_URL, "ignite.network.nodeFinder.netClusterNodes");
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs("[\n"
                        + "    \"localhost:3344\",\n"
                        + "    \"localhost:3345\",\n"
                        + "    \"localhost:3346\"\n"
                        + "]" + System.lineSeparator()
                )
        );
    }
}
