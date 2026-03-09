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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for SQL REPL paged result fetching.
 */
class ItSqlReplPagedResultTest extends CliIntegrationTest {

    private static final String TEST_TABLE = "paged_result_test";
    private static final int TOTAL_ROWS = 50;

    @BeforeEach
    void createTestTable() {
        sql("CREATE TABLE " + TEST_TABLE + " (id INT PRIMARY KEY, name VARCHAR(100), val INT)");

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
    @DisplayName("Should handle paged results with small page size")
    void smallPageSize() {
        execute("cli", "config", "set", "ignite.cli.sql.display-page-size", "10");
        execute("sql", "--jdbc-url", JDBC_URL, "SELECT * FROM " + TEST_TABLE);

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains("ID"),
                () -> assertOutputContains("NAME"),
                () -> assertOutputContains("VAL"),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Should work with default page size")
    void defaultPageSize() {
        execute("sql", "--jdbc-url", JDBC_URL, "SELECT * FROM " + TEST_TABLE);

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains("ID"),
                () -> assertOutputContains("NAME"),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Should fall back to default on invalid page size")
    void invalidPageSize() {
        execute("cli", "config", "set", "ignite.cli.sql.display-page-size", "-10");
        execute("sql", "--jdbc-url", JDBC_URL, "SELECT * FROM " + TEST_TABLE);

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains("ID"),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Should handle single row result")
    void singleRowResult() {
        execute("cli", "config", "set", "ignite.cli.sql.display-page-size", "10");
        execute("sql", "--jdbc-url", JDBC_URL, "SELECT * FROM " + TEST_TABLE + " WHERE id = 1");

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains("Name_1"),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Should handle empty result set")
    void emptyResult() {
        execute("cli", "config", "set", "ignite.cli.sql.display-page-size", "10");
        execute("sql", "--jdbc-url", JDBC_URL, "SELECT * FROM " + TEST_TABLE + " WHERE id > 1000");

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Should handle UPDATE statements")
    void updateStatement() {
        execute("cli", "config", "set", "ignite.cli.sql.display-page-size", "10");
        execute("sql", "--jdbc-url", JDBC_URL, "UPDATE " + TEST_TABLE + " SET val = 999 WHERE id = 1");

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains("Updated"),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Should work with page size of 1")
    void pageSizeOne() {
        execute("cli", "config", "set", "ignite.cli.sql.display-page-size", "1");
        execute("sql", "--jdbc-url", JDBC_URL, "SELECT * FROM " + TEST_TABLE + " LIMIT 5");

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains("ID"),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Should handle large page size")
    void largePageSize() {
        execute("cli", "config", "set", "ignite.cli.sql.display-page-size", "10000");
        execute("sql", "--jdbc-url", JDBC_URL, "SELECT * FROM " + TEST_TABLE);

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains("ID"),
                this::assertErrOutputIsEmpty
        );
    }
}
