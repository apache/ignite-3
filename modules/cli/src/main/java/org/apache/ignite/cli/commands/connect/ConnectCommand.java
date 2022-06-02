package org.apache.ignite.cli.commands.connect;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.call.connect.ConnectCall;
import org.apache.ignite.cli.call.connect.ConnectCallInput;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * Connects to the Ignite 3 node.
 */
@Command(name = "connect", description = "Connect to Ignite 3 node.")
@Singleton
public class ConnectCommand extends BaseCommand {

    /**
     * Cluster url option.
     */
    @Parameters(
            description = "Ignite node url.",
            descriptionKey = "ignite.cluster-url", defaultValue = "http://localhost:10300"
    )
    private String nodeUrl;

    @Spec
    private CommandSpec spec;

    @Inject
    private ConnectCall connectCall;

    /** {@inheritDoc} */
    @Override
    public void run() {
        CallExecutionPipeline.builder(connectCall)
                .inputProvider(() -> ConnectCallInput.builder().nodeUrl(nodeUrl).build())
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .build()
                .runPipeline();
    }
}
