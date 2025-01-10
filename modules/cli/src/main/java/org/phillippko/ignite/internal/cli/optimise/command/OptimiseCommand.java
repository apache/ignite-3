
package org.phillippko.ignite.internal.cli.optimise.command;

import org.apache.ignite.internal.cli.commands.BaseCommand;
import picocli.CommandLine.Command;

@Command(name = "runOptimise",
        subcommands = {
                RunOptimiseCommand.class,
                RunBenchmarkCommand.class,
                GetResultCommand.class
        },
        description = "Tools for optimising cluster configuration.")
public class OptimiseCommand extends BaseCommand {
}
