package org.apache.ignite.cli.commands;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.VersionProvider;
import org.apache.ignite.cli.commands.cliconfig.CliCommand;
import org.apache.ignite.cli.commands.configuration.ConfigCommand;
import org.apache.ignite.cli.commands.sql.SqlCommand;
import org.apache.ignite.cli.commands.status.StatusCommand;
import org.apache.ignite.cli.commands.topology.TopologyCommand;
import org.apache.ignite.cli.deprecated.spec.ClusterCommandSpec;
import org.apache.ignite.cli.deprecated.spec.InitIgniteCommandSpec;
import org.apache.ignite.cli.deprecated.spec.NodeCommandSpec;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Top-level command that prints help and declares subcommands.
 */
@Command(name = "ignite",
        versionProvider = VersionProvider.class,
        description = {
                "Welcome to Ignite Shell alpha.",
                "Run without command to enter interactive mode.",
                ""},
        subcommands = {
                SqlCommand.class,
                CommandLine.HelpCommand.class,
                ConfigCommand.class,
                StatusCommand.class,
                TopologyCommand.class,
                CliCommand.class,
                InitIgniteCommandSpec.class,
                NodeCommandSpec.class,
                ClusterCommandSpec.class
        })
@Singleton
public class TopLevelCliCommand extends BaseCommand {
    @Option(names = {"--version"}, versionHelp = true, description = "Print version information and exit")
    private boolean versionRequested;

    @Override
    public void run() {

    }
}
