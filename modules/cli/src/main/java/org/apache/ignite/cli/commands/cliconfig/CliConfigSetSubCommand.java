package org.apache.ignite.cli.commands.cliconfig;

import jakarta.inject.Inject;
import java.util.Map;
import org.apache.ignite.cli.call.cliconfig.CliConfigSetCall;
import org.apache.ignite.cli.call.cliconfig.CliConfigSetCallInput;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * Command to set CLI configuration parameters.
 */
@Command(name = "set")
public class CliConfigSetSubCommand extends BaseCommand {
    @Parameters(arity = "1..*")
    private Map<String, String> parameters;

    @Inject
    private CliConfigSetCall call;

    @Override
    public void run() {
        CallExecutionPipeline.builder(call)
                .inputProvider(() -> new CliConfigSetCallInput(parameters))
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .build()
                .runPipeline();
    }
}
