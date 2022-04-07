package org.apache.ignite.cli.commands;

import org.apache.ignite.cli.commands.sql.SqlReplCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.shell.jline3.PicocliCommands;

/**
 * Top-level command that just prints help.
 */
@Command(name = "",
        description = {
                "Example interactive shell with completion and autosuggestions. "
                        + "Hit @|magenta <TAB>|@ to see available commands.",
                "Hit @|magenta ALT-S|@ to toggle tailtips.",
                ""},
        footer = {"", "Press Ctrl-D to exit."},
        subcommands = {SqlReplCommand.class, PicocliCommands.ClearScreen.class, CommandLine.HelpCommand.class})
public class CliCommands implements Runnable {
    @Override
    public void run() {
    
    }
}
