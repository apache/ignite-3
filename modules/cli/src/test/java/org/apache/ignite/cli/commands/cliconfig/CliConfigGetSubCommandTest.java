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

import static org.junit.jupiter.api.Assertions.assertAll;

import jakarta.inject.Inject;
import org.apache.ignite.cli.commands.CliCommandTestBase;
import org.apache.ignite.cli.config.ini.IniConfigManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CliConfigGetSubCommandTest extends CliCommandTestBase {
    @Inject
    private TestConfigManagerProvider configManagerProvider;

    @Override
    protected Class<?> getCommandClass() {
        return CliConfigGetSubCommand.class;
    }

    @BeforeEach
    public void setup() {
        configManagerProvider.configManager = new IniConfigManager(TestConfigManagerHelper.createSectionWithInternalPart());
    }

    @Test
    @DisplayName("Key is mandatory")
    void noKey() {
        // When executed without arguments
        execute();

        assertAll(
                () -> assertExitCodeIs(2),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Missing required parameter: '<key>'")
        );
    }

    @Test
    @DisplayName("Displays value for specified key")
    void singleKey() {
        // When executed with single key
        execute("server");

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputIs("127.0.0.1" + System.lineSeparator()),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Displays empty string for nonexistent key")
    void nonexistentKey() {
        // When executed with nonexistent key
        execute("nonexistentKey");

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputIs(System.lineSeparator()),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Only one key is allowed")
    void multipleKeys() {
        // When executed with multiple keys
        execute("ignite.cluster-endpoint-url", "ignite.jdbc-url");

        assertAll(
                () -> assertExitCodeIs(2),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Unmatched argument at index 1")
        );
    }

    @Test
    public void testWithNonDefaultProfile() {
        execute("organization --profile owner");

        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Apache Ignite")
        );

    }

    @Test
    public void testWithNonNotExistedProfile() {
        execute("organization --profile notExist");

        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Profile notExist not found.")
        );
    }
}
