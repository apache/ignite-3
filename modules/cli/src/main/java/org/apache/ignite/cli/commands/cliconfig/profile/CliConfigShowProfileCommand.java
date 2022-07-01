package org.apache.ignite.cli.commands.cliconfig.profile;

import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.apache.ignite.cli.call.cliconfig.profile.CliConfigShowProfileCall;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.call.EmptyCallInput;
import picocli.CommandLine;

/**
 * Show current profile command.
 */
@CommandLine.Command(name = "profile", description = "Show current default profile.")
public class CliConfigShowProfileCommand extends BaseCommand implements Callable<Integer> {

    @Inject
    private CliConfigShowProfileCall call;

    @Override
    public Integer call() throws Exception {
        return CallExecutionPipeline.builder(call)
                .inputProvider(EmptyCallInput::new)
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .build()
                .runPipeline();
    }
}
