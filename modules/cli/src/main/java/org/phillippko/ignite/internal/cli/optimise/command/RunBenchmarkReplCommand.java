package org.phillippko.ignite.internal.cli.optimise.command;

import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_NAME_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_NAME_OPTION_SHORT;

import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlMixin;
import org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.exception.handler.ClusterNotInitializedExceptionHandler;
import org.apache.ignite.internal.cli.core.flow.builder.Flows;
import org.phillippko.ignite.internal.cli.optimise.call.RunBenchmarkCall;
import org.phillippko.ignite.internal.cli.optimise.call.RunBenchmarkCallInput;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Command that shows configuration from the cluster.
 */
@Command(name = "runBenchmark", description = "Shows node configuration")
public class RunBenchmarkReplCommand extends BaseCommand implements Runnable {
    /** Node URL option. */
    @Mixin
    private ClusterUrlMixin clusterUrlMixin;

    @Inject
    private RunBenchmarkCall call;

    @Parameters(
            defaultValue = "defaultBenchmark.sql",
            index = "0",
            description = "Path to the file containing SQL queries to run during benchmark."
    )
    private String benchmarkFilePath;

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

    private RunBenchmarkCallInput buildCallInput(String url) {
        return RunBenchmarkCallInput.builder()
                .setClusterUrl(url)
                .setBenchmarkFilePath(benchmarkFilePath)
                .setNodeName(nodeName)
                .build();
    }
}
