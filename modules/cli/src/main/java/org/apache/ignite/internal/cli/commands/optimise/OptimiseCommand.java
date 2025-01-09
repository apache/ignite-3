
package org.apache.ignite.internal.cli.commands.optimise;

import org.apache.ignite.internal.cli.commands.BaseCommand;
import picocli.CommandLine.Command;

@Command(name = "optimise",
        subcommands = {
                RunOptimiseCommand.class,
                RunBenchmarkCommand.class,
                GetResultCommand.class
        },
        description = "Tools for optimising cluster configuration.")
public class OptimiseCommand extends BaseCommand {
}
