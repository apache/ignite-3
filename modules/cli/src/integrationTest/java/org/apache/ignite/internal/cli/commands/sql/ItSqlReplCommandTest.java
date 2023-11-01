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
import org.apache.ignite.internal.cli.commands.CliCommandTestInitializedIntegrationBase;
import org.apache.ignite.internal.cli.core.repl.executor.ReplExecutorProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests for {@link SqlReplCommand}. */
class ItSqlReplCommandTest extends CliCommandTestInitializedIntegrationBase {
    @Override
    protected Class<?> getCommandClass() {
        return SqlReplCommand.class;
    }

    @Test
    @DisplayName("Should throw error if executed with non-existing file")
    void nonExistingFile() {
        execute("-f", "nonexisting", "--jdbc-url", JDBC_URL);

        assertAll(
                this::assertOutputIsEmpty,
                // Actual output starts with exception since this test doesn't use ReplExecutor and exception is handled by picocli.
                () -> assertErrOutputContains("File with command not found")
        );
    }

    @Test
    void secondInvocationScript() {
        execute("CREATE TABLE T(K INT PRIMARY KEY)", "--jdbc-url", JDBC_URL);

        assertAll(
                () -> assertOutputContains("Updated 0 rows."),
                this::assertErrOutputIsEmpty
        );

        resetOutput();

        execute("--jdbc-url", JDBC_URL);

        assertAll(
                this::assertOutputIsEmpty,
                this::assertErrOutputIsEmpty
        );
    }

    @Test
    void secondInvocationFile() {
        execute("-f", "nonexisting", "--jdbc-url", JDBC_URL);

        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("File with command not found")
        );

        resetOutput();

        execute("--jdbc-url", JDBC_URL);

        assertAll(
                this::assertOutputIsEmpty,
                this::assertErrOutputIsEmpty
        );
    }

    @Bean
    @Replaces(ReplExecutorProvider.class)
    public ReplExecutorProvider replExecutorProvider() {
        return () -> repl -> {};
    }
}
