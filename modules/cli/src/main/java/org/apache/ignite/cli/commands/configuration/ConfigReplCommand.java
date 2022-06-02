package org.apache.ignite.cli.commands.configuration;

import jakarta.inject.Singleton;
import picocli.CommandLine.Command;

/**
 * Parent command for configuration commands.
 */
@Command(name = "config",
        description = "Cluster/node configuration operations.",
        subcommands = {
                ShowConfigReplSubCommand.class,
                UpdateConfigReplSubCommand.class
        })
@Singleton
public class ConfigReplCommand {
}
