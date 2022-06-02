package org.apache.ignite.cli.commands.sql;

import jakarta.inject.Singleton;
import picocli.CommandLine;
import picocli.shell.jline3.PicocliCommands;

/**
 * Top level SQL REPL command.
 */
@CommandLine.Command(name = "",
        description = {""},
        footer = {"", "Press Ctrl-D to exit."},
        subcommands = {
            CommandLine.HelpCommand.class,
            PicocliCommands.ClearScreen.class
        })
@Singleton
public class SqlReplTopLevelCliCommand {
}
