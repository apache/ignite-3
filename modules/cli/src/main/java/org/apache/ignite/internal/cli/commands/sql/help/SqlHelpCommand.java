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

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import org.apache.ignite.internal.cli.core.exception.IgniteCliException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.ColorScheme;
import picocli.CommandLine.IHelpCommandInitializable2;
import picocli.CommandLine.Parameters;

/** Help command in SQL repl mode. */
@Command(name = "help",
        header = "Display help information about the specified SQL command.",
        synopsisHeading = "%nUsage: ",
        helpCommand = true,
        description = {
                "%nWhen no SQL command is given, the usage help for the main command is displayed.",
                "If a SQL command is specified, the help for that command is shown.%n"}
)
public final class SqlHelpCommand implements IHelpCommandInitializable2, Runnable {

    @Parameters(paramLabel = "Ignite SQL command",
            arity = "0..2",
            description = "The SQL command to display the usage help message for.")
    private String[] parameters;

    private CommandLine self;

    private PrintWriter outWriter;

    private ColorScheme colorScheme;

    @Override
    public void run() {
        if (parameters != null) {
            String command = String.join(" ", this.parameters);
            String commandUsage = IgniteSqlCommand.find(command)
                    .map(IgniteSqlCommand::getSyntax)
                    .or(() -> {
                        return Optional.ofNullable(self.getParent())
                                .map(it -> it.getSubcommands().get(command).getUsageMessage());
                    })
                    .orElseThrow(() -> new IgniteCliException("Unknown command: " + command));
            outWriter.println(commandUsage);
        } else {
            String helpMessage = self.getParent().getUsageMessage(colorScheme)
                    + System.lineSeparator()
                    + sqlCommands()
                    + System.lineSeparator()
                    + System.lineSeparator()
                    + "\nPress Ctrl-D to exit";
            outWriter.println(helpMessage);
        }
    }

    private static String sqlCommands() {
        StringJoiner joiner = new StringJoiner(System.lineSeparator());
        joiner.add("SQL commands: ");
        Arrays.stream(IgniteSqlCommand.values())
                .map(IgniteSqlCommand::getTopic)
                .forEach(joiner::add);
        return joiner.toString();
    }

    @Override
    public void init(CommandLine helpCommandLine, ColorScheme colorScheme, PrintWriter out, PrintWriter err) {
        this.self = Objects.requireNonNull(helpCommandLine, "helpCommandLine");
        this.colorScheme = Objects.requireNonNull(colorScheme, "colorScheme");
        this.outWriter = Objects.requireNonNull(out, "outWriter");
        Objects.requireNonNull(err, "errWriter");
    }
}
