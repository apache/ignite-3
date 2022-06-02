package org.apache.ignite.cli.call.configuration;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.core.exception.CommandExecutionException;
import org.apache.ignite.rest.client.api.ClusterConfigurationApi;
import org.apache.ignite.rest.client.api.NodeConfigurationApi;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.invoker.Configuration;
import org.jetbrains.annotations.NotNull;

/**
 * Shows configuration from ignite cluster.
 */
@Singleton
public class ShowConfigurationCall implements Call<ShowConfigurationCallInput, String> {

    /** {@inheritDoc} */
    @Override
    public DefaultCallOutput<String> execute(ShowConfigurationCallInput readConfigurationInput) {
        ApiClient client = createApiClient(readConfigurationInput);

        try {
            return DefaultCallOutput.success(readConfigurationInput.getNodeId() != null
                    ? readNodeConfig(new NodeConfigurationApi(client), readConfigurationInput)
                    : readClusterConfig(new ClusterConfigurationApi(client), readConfigurationInput));
        } catch (ApiException e) {
            return DefaultCallOutput.failure(e);
        } catch (IllegalArgumentException e) {
            return DefaultCallOutput.failure(new CommandExecutionException("show configuration", e.getMessage()));
        }
    }

    private String readNodeConfig(NodeConfigurationApi api, ShowConfigurationCallInput input) throws ApiException {
        return input.getSelector() != null ? api.getNodeConfigurationByPath(input.getSelector()) : api.getNodeConfiguration();
    }

    private String readClusterConfig(ClusterConfigurationApi api, ShowConfigurationCallInput input) throws ApiException {
        return input.getSelector() != null ? api.getClusterConfigurationByPath(input.getSelector()) : api.getClusterConfiguration();
    }

    @NotNull
    private ApiClient createApiClient(ShowConfigurationCallInput input) {
        ApiClient client = Configuration.getDefaultApiClient();
        client.setBasePath(input.getClusterUrl());
        return client;
    }
}
