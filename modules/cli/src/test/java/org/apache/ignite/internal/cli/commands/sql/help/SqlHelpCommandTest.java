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

package org.apache.ignite.internal.cli.commands.sql.help;

import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.ignite.internal.cli.commands.CliCommandTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SqlHelpCommandTest extends CliCommandTestBase {

    @Override
    protected Class<?> getCommandClass() {
        return SqlHelpCommand.class;
    }

    @Test
    @DisplayName("Should throw error if provided wring SQL command")
    void wrongSqlCommand() {
        execute("wrongcommand");

        assertAll(
                () -> assertExitCodeIs(2),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Unknown SQL command:")
        );
    }

    @Test
    @DisplayName("Should display a command syntax if provided a valid command")
    void validSqlCommand() {
        execute(IgniteSqlCommand.SELECT.getTopic());

        assertAll(
                () -> assertExitCodeIs(0),
                () -> assertOutputContains(IgniteSqlCommand.SELECT.getSyntax()),
                this::assertErrOutputIsEmpty);
    }
}
