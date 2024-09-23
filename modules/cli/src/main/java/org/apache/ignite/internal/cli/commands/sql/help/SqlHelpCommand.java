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

import static java.lang.System.lineSeparator;
import static org.apache.ignite.internal.cli.commands.CommandConstants.ABBREVIATE_SYNOPSIS;
import static org.apache.ignite.internal.cli.commands.CommandConstants.COMMAND_LIST_HEADING;
import static org.apache.ignite.internal.cli.commands.CommandConstants.DESCRIPTION_HEADING;
import static org.apache.ignite.internal.cli.commands.CommandConstants.OPTION_LIST_HEADING;
import static org.apache.ignite.internal.cli.commands.CommandConstants.PARAMETER_LIST_HEADING;
import static org.apache.ignite.internal.cli.commands.CommandConstants.REQUIRED_OPTION_MARKER;
import static org.apache.ignite.internal.cli.commands.CommandConstants.SORT_OPTIONS;
import static org.apache.ignite.internal.cli.commands.CommandConstants.SORT_SYNOPSIS;
import static org.apache.ignite.internal.cli.commands.CommandConstants.SYNOPSIS_HEADING;
import static org.apache.ignite.internal.cli.commands.CommandConstants.USAGE_HELP_AUTO_WIDTH;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.core.exception.IgniteCliException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.ColorScheme;
import picocli.CommandLine.IHelpCommandInitializable2;
import picocli.CommandLine.Parameters;

/** Help command in SQL repl mode. */
@Command(name = "help",
        header = "Display help information about the specified SQL command.",
        helpCommand = true,
        description = {
                "%nWhen no SQL command is given, the usage help for the main command is displayed.",
                "If a SQL command is specified, the help for that command is shown.%n"
        },

        descriptionHeading = DESCRIPTION_HEADING,
        optionListHeading = OPTION_LIST_HEADING,
        synopsisHeading = SYNOPSIS_HEADING,
        requiredOptionMarker = REQUIRED_OPTION_MARKER,
        usageHelpAutoWidth = USAGE_HELP_AUTO_WIDTH,
        sortOptions = SORT_OPTIONS,
        sortSynopsis = SORT_SYNOPSIS,
        abbreviateSynopsis = ABBREVIATE_SYNOPSIS,
        commandListHeading = COMMAND_LIST_HEADING,
        parameterListHeading = PARAMETER_LIST_HEADING
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
            String command = String.join(" ", parameters);
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
                    + lineSeparator()
                    + sqlCommands()
                    + lineSeparator()
                    + lineSeparator()
                    + "Press Ctrl-D to exit.";
            outWriter.println(helpMessage);
        }
    }

    private String sqlCommands() {
        Set<String> topicsSet = new HashSet<>();
        List<String> topics = new ArrayList<>();
        for (IgniteSqlCommand command : IgniteSqlCommand.values()) {
            // Take each command only once.
            if (topicsSet.add(command.getTopic().toLowerCase())) {
                topics.add(colorScheme.commandText("  " + command.getTopic()).toString());
            }
        }

        return topics.stream().collect(Collectors.joining(lineSeparator(), "SQL commands" + lineSeparator(), ""));
    }

    @Override
    public void init(CommandLine helpCommandLine, ColorScheme colorScheme, PrintWriter out, PrintWriter err) {
        this.self = Objects.requireNonNull(helpCommandLine, "helpCommandLine");
        this.colorScheme = Objects.requireNonNull(colorScheme, "colorScheme");
        this.outWriter = Objects.requireNonNull(out, "outWriter");
        Objects.requireNonNull(err, "errWriter");
    }
}
