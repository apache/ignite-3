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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SqlCommand}.
 */
class ItSqlCommandTest extends CliSqlCommandTestBase {

    @Test
    @DisplayName("Should throw error if executed with non-existing file")
    void nonExistingFile() {
        execute("sql", "--file", "nonexisting", "--jdbc-url", JDBC_URL);

        assertAll(
                () -> assertExitCodeIs(1),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("nonexisting] not found")
        );
    }

    @Test
    @DisplayName("Should execute select * from table and display table when jdbc-url is correct")
    void selectFromTable() {
        execute("sql", "select * from person", "--jdbc-url", JDBC_URL);

        assertAll(
                this::assertExitCodeIsZero,
                this::assertOutputIsNotEmpty,
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Output should show aliases for columns in sql output")
    void showColumnAliases() {
        execute("sql", "select id, name, salary, salary + 1 from person", "--jdbc-url", JDBC_URL);
        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputIsSqlResultWithColumns("ID", "NAME", "SALARY", "\"SALARY + 1\""),
                this::assertErrOutputIsEmpty
        );

        execute("sql", "select id as col_1, name as col_2, salary as col_3, salary + 1 as col_4 from person", "--jdbc-url", JDBC_URL);
        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputIsSqlResultWithColumns("COL_1", "COL_2", "COL_3", "COL_4"),
                this::assertErrOutputIsEmpty
        );

    }

    @Test
    @DisplayName("Should execute sequence of statements without error")
    void createSelectAlterSelect() {
        String[] statements = {
                "CREATE TABLE test(id INT PRIMARY KEY, val1 INT, val2 INT)",
                "SELECT * FROM test",
                "ALTER TABLE test DROP COLUMN val2",
                "SELECT * FROM test",
        };

        for (String statement : statements) {
            execute("sql", statement, "--jdbc-url", JDBC_URL);

            assertAll(
                    this::assertExitCodeIsZero,
                    this::assertOutputIsNotEmpty,
                    this::assertErrOutputIsEmpty
            );
        }
    }

    @Test
    @DisplayName("Should display readable error when wrong jdbc is given")
    void wrongJdbcUrl() {
        execute("sql", "select * from person", "--jdbc-url", "jdbc:ignite:thin://no-such-host.com:10800");

        assertAll(
                () -> assertExitCodeIs(1),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Connection failed")
        );
    }

    @Test
    @DisplayName("Should display readable error when wrong query is given")
    void incorrectQueryTest() {
        execute("sql", "select", "--jdbc-url", JDBC_URL);

        assertAll(
                () -> assertExitCodeIs(1),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Failed to parse query: Encountered \"<EOF>\" at line 1, column 6")
        );
    }

    @Test
    @DisplayName("Should display readable error when not SQL expression given")
    void notSqlExpression() {
        execute("sql", "asdf", "--jdbc-url", JDBC_URL);

        assertAll(
                () -> assertExitCodeIs(1),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Failed to parse query: Non-query expression encountered in illegal context")
        );
    }

    @Test
    @DisplayName("Should display readable error when select from non existent table")
    void noSuchTableSelect() {
        execute("sql", "select * from nosuchtable", "--jdbc-url", JDBC_URL);

        assertAll(
                () -> assertExitCodeIs(1),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("From line 1, column 15 to line 1, column 25: Object 'NOSUCHTABLE' not found")
        );
    }

    @Test
    @DisplayName("Should display readable error when create table without PK")
    void createTableWithoutPk() {
        execute("sql", "create table mytable(i int)", "--jdbc-url", JDBC_URL);

        assertAll(
                () -> assertExitCodeIs(1),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Table without PRIMARY KEY is not supported")
        );
    }

    @Test
    @DisplayName("Should execute multiline sql script from file")
    void multilineScript() {
        String filePath = getClass().getResource("/multiline.sql").getPath();
        execute("sql", "--file", filePath, "--jdbc-url", JDBC_URL);

        assertAll(
                this::assertExitCodeIsZero,
                this::assertOutputIsNotEmpty,
                this::assertErrOutputIsEmpty
        );

        // test_table is created in multiline.sql and populated with 3 records.
        execute("sql", "select count(*) from test_table", "--jdbc-url", JDBC_URL);

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains("3"),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    void exceptionHandler() {
        execute("sql", "SELECT 1/0;", "--jdbc-url", JDBC_URL);

        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("SQL query execution error"),
                () -> assertErrOutputContains("Division by zero"),
                () -> assertErrOutputDoesNotContain("Unknown error")
        );

        execute("sql", "SELECT * FROM NOTEXISTEDTABLE;", "--jdbc-url", JDBC_URL);

        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("SQL query validation error"),
                () -> assertErrOutputContains("Object 'NOTEXISTEDTABLE' not found"),
                () -> assertErrOutputDoesNotContain("Unknown error")
        );
    }

    @Test
    @DisplayName("An error should be displayed indicating that the script transaction was not completed by the script.")
    void scriptTxNotFinishedByScript() {
        String expectedError = "Transaction block doesn't have a COMMIT statement at the end.";

        {
            execute("sql", "START TRANSACTION;", "--jdbc-url", JDBC_URL);

            assertAll(
                    this::assertOutputIsEmpty,
                    () -> assertErrOutputContains("SQL query execution error"),
                    () -> assertErrOutputContains(expectedError),
                    () -> assertErrOutputDoesNotContain("Unknown error")
            );
        }

        {
            execute("sql", "START TRANSACTION; SELECT 1;", "--jdbc-url", JDBC_URL);

            assertAll(
                    this::assertOutputIsEmpty,
                    () -> assertErrOutputContains("SQL query execution error"),
                    () -> assertErrOutputContains(expectedError),
                    () -> assertErrOutputDoesNotContain("Unknown error")
            );
        }

        {
            execute("sql", "START TRANSACTION; SELECT 1; SELECT 2;", "--jdbc-url", JDBC_URL);

            assertAll(
                    this::assertOutputIsEmpty,
                    () -> assertErrOutputContains("SQL query execution error"),
                    () -> assertErrOutputContains(expectedError),
                    () -> assertErrOutputDoesNotContain("Unknown error")
            );
        }

        {
            execute("sql", "START TRANSACTION; SELECT 1; SELECT 2;", "--jdbc-url", JDBC_URL);

            assertAll(
                    this::assertOutputIsEmpty,
                    () -> assertErrOutputContains("SQL query execution error"),
                    () -> assertErrOutputContains(expectedError),
                    () -> assertErrOutputDoesNotContain("Unknown error")
            );
        }
    }
}
