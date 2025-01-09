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

@Singleton
public class GetResultCall implements Call<GetResultCallInput, String> {
    private final ApiClientFactory clientFactory;

    public GetResultCall(ApiClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    @Override
    public DefaultCallOutput<String> execute(GetResultCallInput input) {
        try {
            OptimiseApi optimiseApi = new OptimiseApi(clientFactory.getClient(input.getClusterUrl()));

            return success(optimiseApi.optimisationResult(input.getId()));
        } catch (ApiException e) {
            return failure(new IgniteCliApiException(e, input.getClusterUrl()));
        }
    }
}
