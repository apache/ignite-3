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

package org.apache.ignite.cli.commands.configuration;


import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.cli.commands.CliCommandTestInitializedIntegrationBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for cluster/node config commands.
 */
class ItConfigCommandTest extends CliCommandTestInitializedIntegrationBase {

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
        execute("cluster", "config", "update", "--cluster-endpoint-url", NODE_URL, "{table: {defaultDataStorage: rocksdb}}");

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
                () -> assertOutputContains("\"defaultDataStorage\" : \"rocksdb\"")
        );
    }

    @Test
    @DisplayName("Should update config with key-value format when valid cluster-endpoint-url is given")
    void updateConfigWithSpecifiedPath() {
        // When update default data storage to rocksdb
        execute("cluster", "config", "update", "--cluster-endpoint-url", NODE_URL, "table.defaultDataStorage=rocksdb");

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
                () -> assertOutputContains("\"defaultDataStorage\" : \"rocksdb\"")
        );
    }
}
