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

import org.junit.jupiter.api.Test;

/**
 * Tests for the --timed option of the sql command.
 */
public class ItSqlTimedOptionTest extends CliSqlCommandTestBase {

    /**
     * Tests that --timed option displays query execution time after SELECT result.
     */
    @Test
    void timedSelectQuery() {
        execute("sql", "select * from person", "--jdbc-url", JDBC_URL, "--timed");

        assertAll(
                this::assertExitCodeIsZero,
                // Should contain the table with data
                () -> assertOutputContains("ID"),
                () -> assertOutputContains("NAME"),
                // Should contain timing information at the end
                () -> assertOutputMatches("(?s).*Query executed in \\d+ms \\(client-side\\)\\.\\s*"),
                this::assertErrOutputIsEmpty
        );
    }

    /**
     * Tests that --timed option displays query execution time after INSERT result.
     */
    @Test
    void timedInsertQuery() {
        execute("sql", "insert into person(id, name, salary) values (100, 'Test', 50.0)", "--jdbc-url", JDBC_URL, "--timed");

        assertAll(
                this::assertExitCodeIsZero,
                // Should contain the update count message
                () -> assertOutputContains("Updated 1 rows."),
                // Should contain timing information at the end
                () -> assertOutputMatches("(?s).*Query executed in \\d+ms \\(client-side\\)\\.\\s*"),
                this::assertErrOutputIsEmpty
        );
    }

    /**
     * Tests that --timed option displays query execution time after multiple statements.
     */
    @Test
    void timedMultipleStatements() {
        execute("sql", "insert into person(id, name, salary) values (101, 'A', 10.0); select count(*) from person",
                "--jdbc-url", JDBC_URL, "--timed");

        assertAll(
                this::assertExitCodeIsZero,
                // Should contain insert result
                () -> assertOutputContains("Updated 1 rows."),
                // Should contain select result (column name)
                () -> assertOutputContains("COUNT(*)"),
                // Should contain timing information at the very end (only once, after all results)
                () -> assertOutputMatches("(?s).*Query executed in \\d+ms \\(client-side\\)\\.\\s*"),
                this::assertErrOutputIsEmpty
        );
    }

    /**
     * Tests that without --timed option, no timing information is displayed.
     */
    @Test
    void noTimedOption() {
        execute("sql", "select * from person", "--jdbc-url", JDBC_URL);

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains("ID"),
                () -> assertOutputDoesNotContain("Query executed in"),
                this::assertErrOutputIsEmpty
        );
    }

    /**
     * Tests that --timed works together with --plain option.
     */
    @Test
    void timedWithPlainOption() {
        execute("sql", "select id from person where id = 0", "--jdbc-url", JDBC_URL, "--timed", "--plain");

        assertAll(
                this::assertExitCodeIsZero,
                // Plain output should not have fancy borders
                () -> assertOutputDoesNotContain("╔"),
                () -> assertOutputDoesNotContain("║"),
                // Should still contain timing information
                () -> assertOutputMatches("(?s).*Query executed in \\d+ms \\(client-side\\)\\.\\s*"),
                this::assertErrOutputIsEmpty
        );
    }

    /**
     * Tests that --timed shows time for DDL queries (CREATE TABLE).
     */
    @Test
    void timedDdlQuery() {
        execute("sql", "create table timed_test(id int primary key)", "--jdbc-url", JDBC_URL, "--timed");

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains("Updated 0 rows."),
                () -> assertOutputMatches("(?s).*Query executed in \\d+ms \\(client-side\\)\\.\\s*"),
                this::assertErrOutputIsEmpty
        );

        // Clean up the created table
        execute("sql", "drop table timed_test", "--jdbc-url", JDBC_URL);
    }
}
