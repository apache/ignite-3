package org.apache.ignite.cli.commands.configuration;

import jakarta.inject.Inject;
import org.apache.ignite.cli.call.configuration.ShowConfigurationCall;
import org.apache.ignite.cli.call.configuration.ShowConfigurationCallInput;
import org.apache.ignite.cli.commands.decorators.JsonDecorator;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.cli.core.repl.Session;
import org.apache.ignite.rest.client.invoker.ApiException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Command that shows configuration from the cluster.
 */
@Command(name = "show",
        description = "Shows configuration.")
public class ShowConfigReplSubCommand implements Runnable {

    /**
     * Node ID option.
     */
    @Option(names = {"--node"}, description = "Node ID to get local configuration.")
    private String nodeId;

    /**
     * Configuration selector option.
     */
    @Option(names = {"--selector"}, description = "Configuration path selector.")
    private String selector;

    /**
     * Cluster url option.
     */
    @Option(
            names = {"--cluster-url"}, description = "Url to cluster node."
    )
    private String clusterUrl;

    @Spec
    private CommandSpec spec;

    @Inject
    private ShowConfigurationCall call;

    @Inject
    private Session session;

    /** {@inheritDoc} */
    @Override
    public void run() {
        var input = ShowConfigurationCallInput.builder().selector(selector).nodeId(nodeId);
        if (session.isConnectedToNode()) {
            input.clusterUrl(session.getNodeUrl());
        } else if (clusterUrl != null) {
            input.clusterUrl(clusterUrl);
        } else {
            spec.commandLine().getErr().println("You are not connected to node. Run 'connect' command or use '--cluster-url' option.");
            return;
        }

        CallExecutionPipeline.builder(call)
                .inputProvider(input::build)
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .decorator(new JsonDecorator())
                .exceptionHandler(new ShowConfigReplExceptionHandler())
                .build()
                .runPipeline();
    }

    private static class ShowConfigReplExceptionHandler implements ExceptionHandler<ApiException> {
        @Override
        public void handle(ExceptionWriter err, ApiException e) {
            err.write("Cannot show config, probably you have not initialized the cluster. Try to run 'cluster init' command.");
        }

        @Override
        public Class<ApiException> applicableException() {
            return ApiException.class;
        }
    }
}
