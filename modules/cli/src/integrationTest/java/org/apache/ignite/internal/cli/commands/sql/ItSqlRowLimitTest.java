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
 * Integration tests for SQL row limiting feature.
 */
class ItSqlRowLimitTest extends CliIntegrationTest {

    private static final String TEST_TABLE = "row_limit_test";
    private static final int TOTAL_ROWS = 100;

    @BeforeEach
    void createTestTable() {
        // Create table
        sql("CREATE TABLE " + TEST_TABLE + " (id INT PRIMARY KEY, name VARCHAR(100))");

        // Insert test data using a single INSERT with multiple VALUES
        StringBuilder insertSql = new StringBuilder("INSERT INTO ").append(TEST_TABLE).append(" VALUES ");
        for (int i = 1; i <= TOTAL_ROWS; i++) {
            if (i > 1) {
                insertSql.append(", ");
            }
            insertSql.append("(").append(i).append(", 'Name_").append(i).append("')");
        }
        sql(insertSql.toString());
    }

    @AfterEach
    void dropTestTable() {
        sql("DROP TABLE IF EXISTS " + TEST_TABLE);
    }

    @Test
    @DisplayName("Query with --limit should return limited rows and show truncation indicator")
    void queryWithLimit() {
        execute("sql", "SELECT * FROM " + TEST_TABLE, "--jdbc-url", JDBC_URL, "--limit", "10");

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains("showing first 10 rows, more available"),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Query with --limit=0 should return all rows without truncation indicator")
    void queryWithUnlimitedLimit() {
        execute("sql", "SELECT * FROM " + TEST_TABLE, "--jdbc-url", JDBC_URL, "--limit", "0");

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputDoesNotContain("showing first"),
                () -> assertOutputDoesNotContain("more available"),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Query with --limit greater than row count should not show truncation indicator")
    void queryWithLimitGreaterThanRows() {
        execute("sql", "SELECT * FROM " + TEST_TABLE, "--jdbc-url", JDBC_URL, "--limit", "500");

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputDoesNotContain("showing first"),
                () -> assertOutputDoesNotContain("more available"),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Query with default limit (1000) should not truncate when rows < 1000")
    void queryWithDefaultLimit() {
        // Default limit is 1000, our table has 100 rows
        execute("sql", "SELECT * FROM " + TEST_TABLE, "--jdbc-url", JDBC_URL);

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputDoesNotContain("showing first"),
                () -> assertOutputDoesNotContain("more available"),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Query with --limit=1 should return single row and show truncation indicator")
    void queryWithLimitOne() {
        execute("sql", "SELECT * FROM " + TEST_TABLE, "--jdbc-url", JDBC_URL, "--limit", "1");

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains("showing first 1 row, more available"),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Truncation indicator shows correct row count")
    void truncationIndicatorShowsCorrectCount() {
        execute("sql", "SELECT * FROM " + TEST_TABLE, "--jdbc-url", JDBC_URL, "--limit", "25");

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains("showing first 25 rows, more available"),
                this::assertErrOutputIsEmpty
        );
    }
}
