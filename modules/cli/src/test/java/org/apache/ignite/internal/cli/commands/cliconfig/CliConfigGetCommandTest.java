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

import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CliConfigGetCommandTest extends CliConfigCommandTestBase {
    @Override
    protected Class<?> getCommandClass() {
        return CliConfigGetCommand.class;
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

        assertSuccessfulOutputIs("127.0.0.1" + System.lineSeparator());
    }

    @Test
    @DisplayName("Displays error for nonexistent key")
    void nonexistentKey() {
        // When executed with nonexistent key
        execute("nonexistentKey");

        assertAll(
                () -> assertExitCodeIs(1),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Property nonexistentKey doesn't exist")
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
