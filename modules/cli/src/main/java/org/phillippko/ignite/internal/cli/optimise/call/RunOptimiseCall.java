package org.phillippko.ignite.internal.cli.optimise.call;

import static org.apache.ignite.internal.cli.core.call.DefaultCallOutput.failure;
import static org.apache.ignite.internal.cli.core.call.DefaultCallOutput.success;

import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.rest.client.api.OptimiseApi;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.RunOptimisationRequest;

@Singleton
public class RunOptimiseCall implements Call<RunOptimiseCallInput, String> {
    private final ApiClientFactory clientFactory;

    public RunOptimiseCall(ApiClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    @Override
    public DefaultCallOutput<String> execute(RunOptimiseCallInput input) {
        try {
            OptimiseApi optimiseApi = new OptimiseApi(clientFactory.getClient(input.getClusterUrl()));

            RunOptimisationRequest request = new RunOptimisationRequest()
                    .writeIntensive(input.getWriteIntensive())
                    .nodeName(input.getNodeName());

            return success("Optimisation was started successfully with id " + optimiseApi.optimise(request));
        } catch (ApiException e) {
            return failure(new IgniteCliApiException(e, input.getClusterUrl()));
        }
    }
}
