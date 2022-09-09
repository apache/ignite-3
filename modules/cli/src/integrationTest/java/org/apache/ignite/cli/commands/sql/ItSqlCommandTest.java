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

package org.apache.ignite.cli.commands.sql;

import static org.apache.ignite.cli.core.exception.handler.SqlExceptionHandler.CLIENT_CONNECTION_FAILED_MESSAGE;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.cli.commands.CliCommandTestInitializedIntegrationBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests for {@link SqlCommand}.
 */
class ItSqlCommandTest extends CliCommandTestInitializedIntegrationBase {
    @BeforeEach
    @Override
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        createAndPopulateTable();
    }

    @AfterEach
    void tearDown() {
        dropAllTables();
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
    @DisplayName("Should display readable error when wrong jdbc is given")
    void wrongJdbcUrl() {
        execute("sql", "select * from person", "--jdbc-url", "jdbc:ignite:thin://no-such-host.com:10800");

        assertAll(
                () -> assertExitCodeIs(1),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains(CLIENT_CONNECTION_FAILED_MESSAGE)
        );
    }

    @Test
    @DisplayName("Should display readable error when wrong query is given")
    void incorrectQueryTest() {
        execute("sql", "select", "--jdbc-url", JDBC_URL);

        assertAll(
                () -> assertExitCodeIs(1),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Failed to parse query: Encountered \"\" at line 1, column 6")
        );
    }

    @Test
    @DisplayName("Should display readable error when wrong engine is given on CREATE TABLE")
    void incorrectEngineOnCreate() {
        execute("sql", "create table mytable1(i int, j int, primary key (i)) with engine='nusuch'", "--jdbc-url", JDBC_URL);

        assertAll(
                () -> assertExitCodeIs(1),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Unexpected table option [option=ENGINE")
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
}
