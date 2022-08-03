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

package org.apache.ignite.cli.commands.cliconfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import jakarta.inject.Inject;
import org.apache.ignite.cli.commands.CliCommandTestBase;
import org.apache.ignite.cli.config.ConfigConstants;
import org.apache.ignite.cli.config.ConfigManager;
import org.apache.ignite.cli.config.ini.IniConfigManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CliConfigSetSubCommandTest extends CliCommandTestBase {

    @Inject
    TestConfigManagerProvider configManagerProvider;

    @Override
    protected Class<?> getCommandClass() {
        return CliConfigSetSubCommand.class;
    }

    @BeforeEach
    public void setup() {
        configManagerProvider.configManager = new IniConfigManager(TestConfigManagerHelper.createSectionWithInternalPart());
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
        execute(ConfigConstants.CLUSTER_URL + "=" + "test");

        assertAll(
                this::assertExitCodeIsZero,
                this::assertOutputIsEmpty,
                this::assertErrOutputIsEmpty,
                () -> assertThat(configManagerProvider.get().getCurrentProperty(ConfigConstants.CLUSTER_URL)).isEqualTo("test")
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
                () -> assertThat(configManager.getCurrentProperty(ConfigConstants.CLUSTER_URL)).isEqualTo("test"),
                () -> assertThat(configManager.getCurrentProperty(ConfigConstants.JDBC_URL)).isEqualTo("test2")
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
                () -> assertThat(configManager.getCurrentProperty(ConfigConstants.CLUSTER_URL)).isNotEqualTo("test"),
                () -> assertThat(configManager.getCurrentProperty(ConfigConstants.JDBC_URL)).isNotEqualTo("test2"),
                () -> assertThat(configManager.getProfile("owner").getProperty(ConfigConstants.CLUSTER_URL)).isEqualTo("test"),
                () -> assertThat(configManager.getProfile("owner").getProperty(ConfigConstants.JDBC_URL)).isEqualTo("test2"));
    }

    @Test
    public void testWithNotExistedProfile() {
        execute("ignite.cluster-endpoint-url=test ignite.jdbc-url=test2 --profile notExist");

        ConfigManager configManager = configManagerProvider.get();

        assertAll(
                () -> assertExitCodeIs(1),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Profile notExist not found"),
                () -> assertThat(configManager.getCurrentProperty(ConfigConstants.CLUSTER_URL)).isNotEqualTo("test")
        );
    }
}
