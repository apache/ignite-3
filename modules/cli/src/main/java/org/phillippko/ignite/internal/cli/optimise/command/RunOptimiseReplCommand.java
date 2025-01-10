package org.phillippko.ignite.internal.cli.optimise.command;

import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_NAME_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_NAME_OPTION_SHORT;
import static org.apache.ignite.internal.cli.commands.Options.Constants.WRITE_INTENSIVE_OPTION;

import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlMixin;
import org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.exception.handler.ClusterNotInitializedExceptionHandler;
import org.apache.ignite.internal.cli.core.flow.builder.Flows;
import org.phillippko.ignite.internal.cli.optimise.call.RunOptimiseCall;
import org.phillippko.ignite.internal.cli.optimise.call.RunOptimiseCallInput;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(name = "runOptimise", description = "Optimise cluster configuration.")
public class RunOptimiseReplCommand extends BaseCommand implements Runnable {
    /** Node URL option. */
    @Mixin
    private ClusterUrlMixin clusterUrlMixin;

    @Inject
    private RunOptimiseCall call;

    @Option(names = WRITE_INTENSIVE_OPTION, description = "If target configuration should be prepared for write-intensive use-cases")
    private boolean writeIntensive;

    @Option(names = {NODE_NAME_OPTION, NODE_NAME_OPTION_SHORT}, description = NODE_NAME_OPTION_DESC)
    private String nodeName;

    @Inject
    private ConnectToClusterQuestion question;

    @Override
    public void run() {
        runFlow(question.askQuestionIfNotConnected(clusterUrlMixin.getClusterUrl())
                .map(this::buildCallInput)
                .then(Flows.fromCall(call))
                .exceptionHandler(ClusterNotInitializedExceptionHandler.createReplHandler("Cannot get results"))
                .print()
        );
    }

    private RunOptimiseCallInput buildCallInput(String url) {
        return RunOptimiseCallInput.builder()
                .setClusterUrl(url)
                .setWriteIntensive(writeIntensive)
                .setNodeName(nodeName)
                .build();
    }
}
