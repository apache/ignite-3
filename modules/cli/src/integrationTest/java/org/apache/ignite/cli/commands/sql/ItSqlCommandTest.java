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
                // TODO: https://issues.apache.org/jira/browse/IGNITE-17090
                () -> assertErrOutputIs(CLIENT_CONNECTION_FAILED_MESSAGE + System.lineSeparator())
        );
    }

    @Test
    @DisplayName("Should display readable error when wrong query is given")
    void incorrectQueryTest() {
        execute("sql", "select", "--jdbc-url", JDBC_URL);

        assertAll(
                () -> assertExitCodeIs(1),
                this::assertOutputIsEmpty,
                // TODO: https://issues.apache.org/jira/browse/IGNITE-17090
                () -> assertErrOutputIs("SQL query parsing error" + System.lineSeparator()
                        + "Sql query execution failed." + System.lineSeparator())
        );
    }
}
