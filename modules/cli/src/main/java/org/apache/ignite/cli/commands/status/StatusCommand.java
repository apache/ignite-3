package org.apache.ignite.cli.commands.status;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.call.status.StatusCall;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.commands.decorators.StatusDecorator;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.call.EmptyCallInput;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Command that prints status of ignite cluster.
 */
@Command(name = "status", description = "Prints status of the cluster.")
@Singleton
public class StatusCommand extends BaseCommand {

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

    @Inject
    private StatusCall statusCall;

    /** {@inheritDoc} */
    @Override
    public void run() {
        CallExecutionPipeline.builder(statusCall)
                .inputProvider(EmptyCallInput::new)
                .output(commandSpec.commandLine().getOut())
                .errOutput(commandSpec.commandLine().getErr())
                .decorator(new StatusDecorator())
                .build()
                .runPipeline();
    }
}
