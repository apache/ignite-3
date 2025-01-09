package org.apache.ignite.internal.cli.commands.optimise;

import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_NAME_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_NAME_OPTION_SHORT;
import static org.apache.ignite.internal.cli.commands.Options.Constants.WRITE_INTENSIVE_OPTION;

import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.call.optimise.RunOptimiseCall;
import org.apache.ignite.internal.cli.call.optimise.RunOptimiseCallInput;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlMixin;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(name = "runOptimise", description = "Optimise cluster configuration.")
public class RunOptimiseCommand extends BaseCommand implements Callable<Integer> {
    /** Node URL option. */
    @Mixin
    private ClusterUrlMixin clusterUrlMixin;

    @Inject
    private RunOptimiseCall call;

    @Option(names = WRITE_INTENSIVE_OPTION, description = "If target configuration should be prepared for write-intensive use-cases")
    private boolean writeIntensive;

    @Option(names = {NODE_NAME_OPTION, NODE_NAME_OPTION_SHORT}, description = NODE_NAME_OPTION_DESC)
    private String nodeName;

    @Override
    public Integer call() {
        return runPipeline(CallExecutionPipeline.builder(call)
                .inputProvider(this::buildCallInput)
                .output(spec.commandLine().getOut())
        );
    }

    private RunOptimiseCallInput buildCallInput() {
        return RunOptimiseCallInput.builder()
                .setClusterUrl(clusterUrlMixin.getClusterUrl())
                .setWriteIntensive(writeIntensive)
                .setNodeName(nodeName)
                .build();
    }
}
