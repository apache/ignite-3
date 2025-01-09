package org.apache.ignite.internal.cli.call.optimise;

import static org.apache.ignite.internal.cli.core.call.DefaultCallOutput.failure;
import static org.apache.ignite.internal.cli.core.call.DefaultCallOutput.success;

import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.rest.client.api.OptimiseApi;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.RunBenchmarkRequest;

@Singleton
public class RunBenchmarkCall implements Call<RunBenchmarkCallInput, String> {
    private final ApiClientFactory clientFactory;

    public RunBenchmarkCall(ApiClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    @Override
    public DefaultCallOutput<String> execute(RunBenchmarkCallInput input) {
        try {
            OptimiseApi optimiseApi = new OptimiseApi(clientFactory.getClient(input.getClusterUrl()));
            RunBenchmarkRequest request = new RunBenchmarkRequest()
                    .benchmarkFilePath(input.getBenchmarkFilePath())
                    .nodeName(input.getNodeName());

            return success("Benchmark was started successfully with id " + optimiseApi.runBenchmark(request));
        } catch (ApiException e) {
            return failure(new IgniteCliApiException(e, input.getClusterUrl()));
        }
    }
}
