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

import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.internal.cli.commands.CliCommandTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ConnectCommandTest extends CliCommandTestBase {

    @Override
    protected Class<?> getCommandClass() {
        return ConnectReplCommand.class;
    }

    @Test
    void invalidNodeUrl() {
        execute("nodeName");

        assertAll(
                () -> assertExitCodeIs(2),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Invalid URL 'nodeName' (no protocol: nodeName)")
        );
    }

    @Test
    @DisplayName("Should throw error if only username provided")
    void usernameOnly() {
        execute("http://localhost:1111 --username user ");

        assertAll(
                () -> assertExitCodeIs(2),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Error: Missing required argument(s): --password=<password>")
        );
    }

    @Test
    @DisplayName("Should throw error if only short username provided")
    void shortUsernameOnly() {
        execute("http://localhost:1111 -u user ");

        assertAll(
                () -> assertExitCodeIs(2),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Error: Missing required argument(s): --password=<password>")
        );
    }

    @Test
    @DisplayName("Should throw error if only password provided")
    void passwordOnly() {
        execute("http://localhost:1111 --password password");

        assertAll(
                () -> assertExitCodeIs(2),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Error: Missing required argument(s): --username=<username>")
        );
    }

    @Test
    @DisplayName("Should throw error if only short password provided")
    void shortPasswordOnly() {
        execute("http://localhost:1111 -p password");

        assertAll(
                () -> assertExitCodeIs(2),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Error: Missing required argument(s): --username=<username>")
        );
    }
}
