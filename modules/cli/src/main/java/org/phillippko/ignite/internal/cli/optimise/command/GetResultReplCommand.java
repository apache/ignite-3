package org.phillippko.ignite.internal.cli.optimise.command;

import static org.apache.ignite.internal.cli.commands.Options.Constants.ID_OPTION_DESC;

import jakarta.inject.Inject;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.call.recovery.restart.RestartPartitionsCall;
import org.apache.ignite.internal.cli.call.recovery.restart.RestartPartitionsCallInput;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlMixin;
import org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.exception.handler.ClusterNotInitializedExceptionHandler;
import org.apache.ignite.internal.cli.core.flow.builder.Flows;
import org.phillippko.ignite.internal.cli.optimise.call.GetResultCall;
import org.phillippko.ignite.internal.cli.optimise.call.GetResultCallInput;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(name = "result", description = "Prints results of the optimisation or benchmark.")
public class GetResultReplCommand extends BaseCommand implements Runnable  {
    @Inject
    private GetResultCall call;

    @Mixin
    private ClusterUrlMixin clusterUrl;

    @Parameters(index = "0", description = ID_OPTION_DESC)
    private UUID id;

    @Inject
    private ConnectToClusterQuestion question;

    @Override
    public void run() {
        runFlow(question.askQuestionIfNotConnected(clusterUrl.getClusterUrl())
                .map(this::buildCallInput)
                .then(Flows.fromCall(call))
                .exceptionHandler(ClusterNotInitializedExceptionHandler.createReplHandler("Cannot get results"))
                .print()
        );
    }

    private GetResultCallInput buildCallInput(String url) {
        return GetResultCallInput.builder()
                .setClusterUrl(url)
                .setId(id)
                .build();
    }
}
