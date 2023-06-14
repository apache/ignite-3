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

import java.util.stream.Stream;
import org.apache.ignite.internal.cli.commands.CliCommandTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SqlHelpCommandTest extends CliCommandTestBase {

    private static Stream<Arguments> sqlCommands() {
        return Stream.of(IgniteSqlCommand.values())
                .map(command -> Arguments.of(command.getTopic(), command.getSyntax()));
    }

    @Override
    protected Class<?> getCommandClass() {
        return SqlHelpCommand.class;
    }

    @Test
    @DisplayName("Should throw error if provided wring SQL command")
    void wrongSqlCommand() {
        execute("wrongcommand");

        assertAll(
                () -> assertExitCodeIs(1),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Unknown command:")
        );
    }

    @ParameterizedTest
    @MethodSource("sqlCommands")
    @DisplayName("Should display a command syntax if provided a valid command")
    void validSqlCommand(String commandToShowHelpFor, String expectedHelpMessage) {
        execute(commandToShowHelpFor);

        assertAll(
                () -> assertExitCodeIs(0),
                () -> assertOutputContains(expectedHelpMessage),
                this::assertErrOutputIsEmpty);
    }
}
