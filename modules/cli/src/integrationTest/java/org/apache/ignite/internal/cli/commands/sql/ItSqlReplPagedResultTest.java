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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for SQL REPL paged result fetching.
 */
class ItSqlReplPagedResultTest extends CliIntegrationTest {

    private static final String TEST_TABLE = "paged_result_test";
    private static final int TOTAL_ROWS = 50;

    @TempDir
    Path tempDir;

    @BeforeEach
    void createTestTable() {
        // Create table
        sql("CREATE TABLE " + TEST_TABLE + " (id INT PRIMARY KEY, name VARCHAR(100), value INT)");

        // Insert test data
        StringBuilder insertSql = new StringBuilder("INSERT INTO ").append(TEST_TABLE).append(" VALUES ");
        for (int i = 1; i <= TOTAL_ROWS; i++) {
            if (i > 1) {
                insertSql.append(", ");
            }
            insertSql.append("(").append(i).append(", 'Name_").append(i).append("', ").append(i * 10).append(")");
        }
        sql(insertSql.toString());
    }

    @AfterEach
    void dropTestTable() {
        sql("DROP TABLE IF EXISTS " + TEST_TABLE);
    }

    @Test
    @DisplayName("SQL REPL mode should handle paged results with small page size")
    void sqlReplWithSmallPageSize() throws IOException {
        // Create a script file with SELECT query
        Path scriptFile = tempDir.resolve("test.sql");
        Files.writeString(scriptFile, "SELECT * FROM " + TEST_TABLE + ";");

        // Set page size to 10
        execute("cli", "config", "set", "ignite.cli.sql.display-page-size", "10");

        // Execute SQL in non-interactive mode (will use paged fetching)
        execute("sql", "--jdbc-url", JDBC_URL, "--script-file", scriptFile.toString());

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains("id"),
                () -> assertOutputContains("name"),
                () -> assertOutputContains("value"),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("SQL REPL should work with default page size")
    void sqlReplWithDefaultPageSize() throws IOException {
        Path scriptFile = tempDir.resolve("test.sql");
        Files.writeString(scriptFile, "SELECT * FROM " + TEST_TABLE + ";");

        // Use default page size (1000)
        execute("sql", "--jdbc-url", JDBC_URL, "--script-file", scriptFile.toString());

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains("id"),
                () -> assertOutputContains("name"),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("SQL REPL should validate page size configuration")
    void sqlReplWithInvalidPageSize() throws IOException {
        Path scriptFile = tempDir.resolve("test.sql");
        Files.writeString(scriptFile, "SELECT * FROM " + TEST_TABLE + ";");

        // Set invalid page size (negative)
        execute("cli", "config", "set", "ignite.cli.sql.display-page-size", "-10");

        // Execute SQL - should fall back to default
        execute("sql", "--jdbc-url", JDBC_URL, "--script-file", scriptFile.toString());

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains("id"),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("SQL REPL should handle single row result")
    void sqlReplWithSingleRowResult() throws IOException {
        Path scriptFile = tempDir.resolve("test.sql");
        Files.writeString(scriptFile, "SELECT * FROM " + TEST_TABLE + " WHERE id = 1;");

        execute("cli", "config", "set", "ignite.cli.sql.display-page-size", "10");
        execute("sql", "--jdbc-url", JDBC_URL, "--script-file", scriptFile.toString());

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains("Name_1"),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("SQL REPL should handle empty result set")
    void sqlReplWithEmptyResult() throws IOException {
        Path scriptFile = tempDir.resolve("test.sql");
        Files.writeString(scriptFile, "SELECT * FROM " + TEST_TABLE + " WHERE id > 1000;");

        execute("cli", "config", "set", "ignite.cli.sql.display-page-size", "10");
        execute("sql", "--jdbc-url", JDBC_URL, "--script-file", scriptFile.toString());

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("SQL REPL should handle UPDATE statements correctly")
    void sqlReplWithUpdateStatement() throws IOException {
        Path scriptFile = tempDir.resolve("test.sql");
        Files.writeString(scriptFile, "UPDATE " + TEST_TABLE + " SET value = 999 WHERE id = 1;");

        execute("cli", "config", "set", "ignite.cli.sql.display-page-size", "10");
        execute("sql", "--jdbc-url", JDBC_URL, "--script-file", scriptFile.toString());

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains("Updated"),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("SQL REPL should work with page size of 1")
    void sqlReplWithPageSizeOne() throws IOException {
        Path scriptFile = tempDir.resolve("test.sql");
        Files.writeString(scriptFile, "SELECT * FROM " + TEST_TABLE + " LIMIT 5;");

        execute("cli", "config", "set", "ignite.cli.sql.display-page-size", "1");
        execute("sql", "--jdbc-url", JDBC_URL, "--script-file", scriptFile.toString());

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains("id"),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("SQL REPL should handle large page size")
    void sqlReplWithLargePageSize() throws IOException {
        Path scriptFile = tempDir.resolve("test.sql");
        Files.writeString(scriptFile, "SELECT * FROM " + TEST_TABLE + ";");

        execute("cli", "config", "set", "ignite.cli.sql.display-page-size", "10000");
        execute("sql", "--jdbc-url", JDBC_URL, "--script-file", scriptFile.toString());

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains("id"),
                this::assertErrOutputIsEmpty
        );
    }
}
