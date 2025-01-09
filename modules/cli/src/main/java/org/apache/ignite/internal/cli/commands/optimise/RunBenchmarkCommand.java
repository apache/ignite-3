package org.apache.ignite.internal.cli.commands.optimise;

import static org.apache.ignite.internal.cli.commands.Options.Constants.BENCHMARK_FILE_PATH_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_NAME_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_NAME_OPTION_SHORT;

import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.call.optimise.RunBenchmarkCall;
import org.apache.ignite.internal.cli.call.optimise.RunBenchmarkCallInput;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlMixin;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/**
 * Command that shows configuration from the cluster.
 */
@Command(name = "runBenchmark", description = "Shows node configuration")
public class RunBenchmarkCommand extends BaseCommand implements Callable<Integer> {
    /** Node URL option. */
    @Mixin
    private ClusterUrlMixin clusterUrlMixin;

    @Inject
    private RunBenchmarkCall call;

    @Option(names = BENCHMARK_FILE_PATH_OPTION, description = "Path to the file containing SQL queries to run during benchmark.")
    private String benchmarkFilePath;

    @Option(names = {NODE_NAME_OPTION, NODE_NAME_OPTION_SHORT}, description = NODE_NAME_OPTION_DESC)
    private String nodeName;

    @Override
    public Integer call() {
        return runPipeline(CallExecutionPipeline.builder(call)
                .inputProvider(this::buildCallInput)
                .output(spec.commandLine().getOut())
        );
    }

    private RunBenchmarkCallInput buildCallInput() {
        return RunBenchmarkCallInput.builder()
                .setClusterUrl(clusterUrlMixin.getClusterUrl())
                .setBenchmarkFilePath(benchmarkFilePath)
                .setNodeName(nodeName)
                .build();
    }
}
