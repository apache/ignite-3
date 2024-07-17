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

package org.apache.ignite.internal.cli.commands.cliconfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.ConfigManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CliConfigSetCommandTest extends CliConfigCommandTestBase {
    @Override
    protected Class<?> getCommandClass() {
        return CliConfigSetCommand.class;
    }

    @Test
    @DisplayName("Need at least one parameter")
    void noKey() {
        // When executed without arguments
        execute();

        assertAll(
                () -> assertExitCodeIs(2),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Missing required parameter")
        );
    }

    @Test
    @DisplayName("Missing value")
    void singleKey() {
        // When executed with key
        execute("ignite.cluster-endpoint-url");

        assertAll(
                () -> assertExitCodeIs(2),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("should be in KEY=VALUE format but was ignite.cluster-endpoint-url")
        );
    }

    @Test
    @DisplayName("Sets single value")
    void singleKeySet() {
        // When executed with key
        execute(CliConfigKeys.CLUSTER_URL.value() + "=" + "test");

        assertAll(
                this::assertExitCodeIsZero,
                this::assertOutputIsEmpty,
                this::assertErrOutputIsEmpty,
                () -> assertThat(configManagerProvider.get().getCurrentProperty(CliConfigKeys.CLUSTER_URL.value())).isEqualTo("test")
        );
    }

    @Test
    @DisplayName("Sets multiple values")
    void multipleKeys() {
        // When executed with multiple keys
        execute("ignite.cluster-endpoint-url=test", "ignite.jdbc-url=test2");

        ConfigManager configManager = configManagerProvider.get();

        assertAll(
                this::assertExitCodeIsZero,
                this::assertOutputIsEmpty,
                this::assertErrOutputIsEmpty,
                () -> assertThat(configManager.getCurrentProperty(CliConfigKeys.CLUSTER_URL.value())).isEqualTo("test"),
                () -> assertThat(configManager.getCurrentProperty(CliConfigKeys.JDBC_URL.value())).isEqualTo("test2")
        );
    }

    @Test
    @DisplayName("Set with profile")
    public void testWithProfile() {
        // When executed with multiple keys
        execute("ignite.cluster-endpoint-url=test ignite.jdbc-url=test2 --profile owner");

        ConfigManager configManager = configManagerProvider.get();

        assertAll(
                this::assertExitCodeIsZero,
                this::assertOutputIsEmpty,
                this::assertErrOutputIsEmpty,
                () -> assertThat(configManager.getCurrentProperty(CliConfigKeys.CLUSTER_URL.value())).isNotEqualTo("test"),
                () -> assertThat(configManager.getCurrentProperty(CliConfigKeys.JDBC_URL.value())).isNotEqualTo("test2"),
                () -> assertThat(configManager.getProfile("owner").getProperty(CliConfigKeys.CLUSTER_URL.value())).isEqualTo("test"),
                () -> assertThat(configManager.getProfile("owner").getProperty(CliConfigKeys.JDBC_URL.value())).isEqualTo("test2"));
    }

    @Test
    public void testWithNotExistedProfile() {
        execute("ignite.cluster-endpoint-url=test ignite.jdbc-url=test2 --profile notExist");

        ConfigManager configManager = configManagerProvider.get();

        assertAll(
                () -> assertExitCodeIs(1),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Profile notExist not found"),
                () -> assertThat(configManager.getCurrentProperty(CliConfigKeys.CLUSTER_URL.value())).isNotEqualTo("test")
        );
    }
}
