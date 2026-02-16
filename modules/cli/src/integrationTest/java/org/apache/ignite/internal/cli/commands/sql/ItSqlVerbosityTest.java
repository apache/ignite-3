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

package org.apache.ignite.internal.cli.commands.sql;

import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for SQL verbose output ({@code -v}, {@code -vv}, {@code -vvv}).
 */
class ItSqlVerbosityTest extends CliIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    @DisplayName("No verbose flag should not produce stderr output")
    void noVerbose() {
        execute("sql", "select 1", "--jdbc-url", JDBC_URL);

        assertAll(
                this::assertExitCodeIsZero,
                this::assertOutputIsNotEmpty,
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("-v should show JDBC URL and SQL query in stderr")
    void verboseBasic() {
        execute("sql", "select 1", "--jdbc-url", JDBC_URL, "-v");

        assertAll(
                this::assertExitCodeIsZero,
                this::assertOutputIsNotEmpty,
                () -> assertErrOutputContains("--> JDBC"),
                () -> assertErrOutputContains("--> SQL select 1"),
                () -> assertErrOutputContains("<-- 1 row(s)"),
                () -> assertErrOutputDoesNotContain("<-- Columns:"),
                () -> assertErrOutputDoesNotContain("--> Driver:")
        );
    }

    @Test
    @DisplayName("-vv should additionally show column metadata")
    void verboseHeaders() {
        execute("sql", "select 1", "--jdbc-url", JDBC_URL, "-vv");

        assertAll(
                this::assertExitCodeIsZero,
                this::assertOutputIsNotEmpty,
                () -> assertErrOutputContains("--> JDBC"),
                () -> assertErrOutputContains("--> SQL select 1"),
                () -> assertErrOutputContains("<-- Columns:"),
                () -> assertErrOutputDoesNotContain("--> Driver:")
        );
    }

    @Test
    @DisplayName("-vvv should additionally show driver info")
    void verboseBody() {
        execute("sql", "select 1", "--jdbc-url", JDBC_URL, "-vvv");

        assertAll(
                this::assertExitCodeIsZero,
                this::assertOutputIsNotEmpty,
                () -> assertErrOutputContains("--> JDBC"),
                () -> assertErrOutputContains("--> Driver:"),
                () -> assertErrOutputContains("--> SQL select 1"),
                () -> assertErrOutputContains("<-- Columns:")
        );
    }
}
