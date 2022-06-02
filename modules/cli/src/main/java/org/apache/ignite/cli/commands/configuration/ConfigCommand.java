package org.apache.ignite.cli.commands.configuration;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.commands.BaseCommand;
import picocli.CommandLine.Command;

/**
 * Parent command for configuration commands.
 */
@Command(name = "config",
        description = "Cluster/node configuration operations.",
        subcommands = {
                ShowConfigSubCommand.class,
                UpdateConfigSubCommand.class
        })
@Singleton
public class ConfigCommand extends BaseCommand {
}
