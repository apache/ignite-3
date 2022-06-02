package org.apache.ignite.cli.commands.connect;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.call.connect.DisconnectCall;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.call.EmptyCallInput;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/**
 * Connects to the Ignite 3 node.
 */
@Command(name = "disconnect", description = "Disconnect from Ignite 3 node.")
@Singleton
public class DisconnectCommand extends BaseCommand {
    @Spec
    private CommandSpec spec;

    @Inject
    private DisconnectCall disconnectCall;

    /** {@inheritDoc} */
    @Override
    public void run() {
        CallExecutionPipeline.builder(disconnectCall)
                .inputProvider(EmptyCallInput::new)
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .build()
                .runPipeline();
    }
}
