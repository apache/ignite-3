package org.phillippko.ignite.internal.cli.optimise.command;

import static org.apache.ignite.internal.cli.commands.Options.Constants.ID_OPTION_DESC;

import jakarta.inject.Inject;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlMixin;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.phillippko.ignite.internal.cli.optimise.call.GetResultCall;
import org.phillippko.ignite.internal.cli.optimise.call.GetResultCallInput;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(name = "result", description = "Prints results of the optimisation or benchmark.")
public class GetResultCommand extends BaseCommand implements Callable<Integer> {
    @Mixin
    private ClusterUrlMixin clusterUrlMixin;

    @Inject
    private GetResultCall call;

    @Parameters(index = "0", description = ID_OPTION_DESC)
    private UUID id;

    @Override
    public Integer call() {
        return runPipeline(CallExecutionPipeline.builder(call)
                .inputProvider(this::buildCallInput)
                .output(spec.commandLine().getOut())
        );
    }

    private GetResultCallInput buildCallInput() {
        return GetResultCallInput.builder()
                .setClusterUrl(clusterUrlMixin.getClusterUrl())
                .setId(id)
                .build();
    }
}
