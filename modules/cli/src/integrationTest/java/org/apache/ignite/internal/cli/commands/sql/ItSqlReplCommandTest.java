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

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Replaces;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.apache.ignite.internal.cli.core.repl.executor.ReplExecutorProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests for {@link SqlReplCommand}. */
class ItSqlReplCommandTest extends CliIntegrationTest {
    @Override
    protected Class<?> getCommandClass() {
        return SqlReplCommand.class;
    }

    @Test
    @DisplayName("Should throw error if executed with non-existing file")
    void nonExistingFile() {
        execute("--file", "nonexisting", "--jdbc-url", JDBC_URL);

        assertAll(
                this::assertOutputIsEmpty,
                // Actual output starts with exception since this test doesn't use ReplExecutor and exception is handled by picocli.
                () -> assertErrOutputContains("nonexisting] not found")
        );
    }

    @Test
    void secondInvocationScript() {
        execute("CREATE TABLE T(K INT PRIMARY KEY)", "--jdbc-url", JDBC_URL);

        assertAll(
                () -> assertOutputContains("Updated 0 rows."),
                this::assertErrOutputIsEmpty
        );

        execute("--jdbc-url", JDBC_URL);

        assertAll(
                this::assertOutputIsEmpty,
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    void multilineCommand() {
        execute("CREATE TABLE MULTILINE_TABLE(K INT PRIMARY KEY); \n INSERT INTO MULTILINE_TABLE VALUES(1);", "--jdbc-url", JDBC_URL);

        assertAll(
                // The output from CREATE TABLE is: Updated 0 rows.
                () -> assertOutputContains("Updated 0 rows."),
                this::assertErrOutputIsEmpty
        );

        execute("SELECT COUNT(*) FROM MULTILINE_TABLE;", "--jdbc-url", JDBC_URL);

        assertAll(
                () -> assertOutputContains("1"),
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    void secondInvocationFile() {
        execute("--file", "nonexisting", "--jdbc-url", JDBC_URL);

        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("nonexisting] not found")
        );

        execute("--jdbc-url", JDBC_URL);

        assertAll(
                this::assertOutputIsEmpty,
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    void exceptionHandler() {
        execute("SELECT 1/0;", "--jdbc-url", JDBC_URL);

        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("SQL query execution error"),
                () -> assertErrOutputContains("Division by zero"),
                () -> assertErrOutputDoesNotContain("Unknown error")
        );

        execute("SELECT * FROM NOTEXISTEDTABLE;", "--jdbc-url", JDBC_URL);

        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("SQL query validation error"),
                () -> assertErrOutputContains("Object 'NOTEXISTEDTABLE' not found"),
                () -> assertErrOutputDoesNotContain("Unknown error")
        );
    }

    @Bean
    @Replaces(ReplExecutorProvider.class)
    public ReplExecutorProvider replExecutorProvider() {
        return () -> repl -> {};
    }
}
