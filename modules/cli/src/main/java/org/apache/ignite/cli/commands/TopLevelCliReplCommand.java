package org.apache.ignite.cli.commands;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.commands.cliconfig.CliCommand;
import org.apache.ignite.cli.commands.configuration.ConfigReplCommand;
import org.apache.ignite.cli.commands.connect.ConnectCommand;
import org.apache.ignite.cli.commands.connect.DisconnectCommand;
import org.apache.ignite.cli.commands.sql.SqlCommand;
import org.apache.ignite.cli.commands.status.StatusCommand;
import org.apache.ignite.cli.commands.topology.TopologyCommand;
import org.apache.ignite.cli.commands.version.VersionCommand;
import org.apache.ignite.cli.deprecated.spec.ClusterReplCommandSpec;
import org.apache.ignite.cli.deprecated.spec.InitIgniteCommandSpec;
import org.apache.ignite.cli.deprecated.spec.NodeCommandSpec;
import picocli.CommandLine;
import picocli.shell.jline3.PicocliCommands;

/**
 * Top-level command that just prints help.
 */
@CommandLine.Command(name = "",
        footer = {"", "Press Ctrl-D to exit."},
        subcommands = {
                SqlCommand.class,
                PicocliCommands.ClearScreen.class,
                CommandLine.HelpCommand.class,
                ConfigReplCommand.class,
                VersionCommand.class,
                StatusCommand.class,
                TopologyCommand.class,
                CliCommand.class,
                InitIgniteCommandSpec.class,
                NodeCommandSpec.class,
                ClusterReplCommandSpec.class,
                ConnectCommand.class,
                DisconnectCommand.class
        })
@Singleton
public class TopLevelCliReplCommand {
}
