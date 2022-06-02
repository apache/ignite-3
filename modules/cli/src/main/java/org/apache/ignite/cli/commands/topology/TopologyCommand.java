package org.apache.ignite.cli.commands.topology;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.commands.BaseCommand;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Command that prints ignite cluster topology.
 */
@Command(name = "topology", description = "Prints topology information.")
@Singleton
public class TopologyCommand extends BaseCommand {

    /**
     * Cluster url option.
     */
    @Option(
            names = {"--cluster-url"}, description = "Url to cluster node.",
            descriptionKey = "ignite.cluster-url", defaultValue = "http://localhost:10300"
    )
    private String clusterUrl;

    @Spec
    private CommandSpec commandSpec;

    /** {@inheritDoc} */
    @Override
    public void run() {
        commandSpec.commandLine().getOut().println("Topology command is not implemented yet.");
    }
}
