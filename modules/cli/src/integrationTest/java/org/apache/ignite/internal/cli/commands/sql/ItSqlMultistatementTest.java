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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for sql multi statement queries.
 */
public class ItSqlMultistatementTest extends CliSqlCommandTestBase {

    @BeforeEach
    void setup() {
        execute("sql", "create table mytable(id int primary key, name varchar)", "--jdbc-url", JDBC_URL);
    }

    /**
     * Test for multiple insert queries. It ensures that the output contains the number of updated rows for each query.
     */
    @Test
    void sequentialUpdates() {
        String testQuery = "insert into mytable(id) values (1);insert into mytable(id) values (2), (3)";
        String expectedOutput = "Updated 1 rows.\n"
                + "Updated 2 rows.";

        execute("sql", testQuery, "--jdbc-url", JDBC_URL);

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains(expectedOutput),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    void sequentialSelects() {
        String testQuery = "select * from mytable order by id; insert into mytable(id) values(3); select * from mytable order by id;";
        String expectedOutput =
                  "╔════╤═══════╗\n"
                + "║ ID │ NAME  ║\n"
                + "╠════╪═══════╣\n"
                + "║ 1  │ Name1 ║\n"
                + "╟────┼───────╢\n"
                + "║ 2  │ Name2 ║\n"
                + "╚════╧═══════╝\n"
                + "Updated 1 rows.\n"
                + "╔════╤═══════╗\n"
                + "║ ID │ NAME  ║\n"
                + "╠════╪═══════╣\n"
                + "║ 1  │ Name1 ║\n"
                + "╟────┼───────╢\n"
                + "║ 2  │ Name2 ║\n"
                + "╟────┼───────╢\n"
                + "║ 3  │ null  ║\n"
                + "╚════╧═══════╝";

        execute("sql", "insert into mytable(id, name) values (1, 'Name1'), (2, 'Name2')", "--jdbc-url", JDBC_URL);

        execute("sql", testQuery, "--jdbc-url", JDBC_URL);

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains(expectedOutput),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    void deleteAfterInserts() {
        String testQuery = "insert into mytable(id) values (1);insert into mytable(id) values (2), (3); delete from mytable;";
        String expectedOutput = "Updated 1 rows.\n"
                + "Updated 2 rows.\n"
                + "Updated 3 rows.";

        execute("sql", testQuery, "--jdbc-url", JDBC_URL);

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains(expectedOutput),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    void updateAfterInserts() {
        String testQuery = "insert into mytable(id) values (1);insert into mytable(id) values (2), (3); update mytable set Name = 'Name1';";
        String expectedOutput = "Updated 1 rows.\n"
                + "Updated 2 rows.\n"
                + "Updated 3 rows.";

        execute("sql", testQuery, "--jdbc-url", JDBC_URL);

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains(expectedOutput),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    void sequentialCreateTable() {
        String testQuery = "create table mytable1(id int primary key); create table mytable2(id int primary key)";
        String expectedOutput = "Updated 0 rows.\n"
                + "Updated 0 rows.";

        execute("sql", testQuery, "--jdbc-url", JDBC_URL);

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains(expectedOutput),
                this::assertErrOutputIsEmpty
        );
    }

    /**
     * Test for single select query. It ensures that there is no unnecessary data in output.
     */
    @Test
    void singleSelect() {
        String testQuery = "select * from mytable";
        String expectedOutput =
                  "╔════╤══════╗\n"
                + "║ ID │ NAME ║\n"
                + "╠════╧══════╣\n"
                + "║ (empty)   ║\n"
                + "╚═══════════╝\n" + System.lineSeparator();

        execute("sql", testQuery, "--jdbc-url", JDBC_URL);

        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputIs(expectedOutput),
                this::assertErrOutputIsEmpty
        );
    }
}
