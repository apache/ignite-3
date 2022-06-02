package org.apache.ignite.cli.commands.cliconfig;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.call.cliconfig.CliConfigCall;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.commands.decorators.ConfigDecorator;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.call.EmptyCallInput;
import picocli.CommandLine.Command;

/**
 * Parent command for CLI configuration commands.
 */
@Command(name = "config", subcommands = {
        CliConfigGetSubCommand.class,
        CliConfigSetSubCommand.class
})
@Singleton
public class CliConfigSubCommand extends BaseCommand {

    @Inject
    private CliConfigCall call;

    @Override
    public void run() {
        CallExecutionPipeline.builder(call)
                .inputProvider(EmptyCallInput::new)
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .decorator(new ConfigDecorator())
                .build()
                .runPipeline();
    }
}
