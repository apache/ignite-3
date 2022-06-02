package org.apache.ignite.cli.commands.cliconfig;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.commands.BaseCommand;
import picocli.CommandLine.Command;

/**
 * Parent command for CLI configuration commands.
 */
@Command(name = "cli",
        description = "CLI specific commands",
        subcommands = {
                CliConfigSubCommand.class
        })
@Singleton
public class CliCommand extends BaseCommand {
}
