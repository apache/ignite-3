package org.apache.ignite.cli.call.status;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.core.call.EmptyCallInput;
import org.apache.ignite.cli.core.repl.Session;
import org.apache.ignite.cli.deprecated.CliPathsConfigLoader;
import org.apache.ignite.cli.deprecated.IgnitePaths;
import org.apache.ignite.cli.deprecated.builtins.node.NodeManager;
import org.apache.ignite.rest.client.api.ClusterConfigurationApi;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.invoker.Configuration;
import org.jetbrains.annotations.NotNull;

/**
 * Call to get cluster status.
 */
@Singleton
public class StatusCall implements Call<EmptyCallInput, Status> {
    private final NodeManager nodeManager;
    private final CliPathsConfigLoader cliPathsCfgLdr;
    private final Session session;

    /**
     * Default constructor.
     */
    public StatusCall(NodeManager nodeManager, CliPathsConfigLoader cliPathsCfgLdr, Session session) {
        this.nodeManager = nodeManager;
        this.cliPathsCfgLdr = cliPathsCfgLdr;
        this.session = session;
    }

    @Override
    public CallOutput<Status> execute(EmptyCallInput input) {
        IgnitePaths paths = cliPathsCfgLdr.loadIgnitePathsOrThrowError();
        return DefaultCallOutput.success(
                Status.builder()
                        .connected(session.isConnectedToNode())
                        .connectedNodeUrl(session.getNodeUrl())
                        .initialized(session.isConnectedToNode() && canReadClusterConfig())
                        .nodeCount(nodeManager.getRunningNodes(paths.logDir, paths.cliPidsDir()).size())
                        .name("CHANGE_ME").build()
        );
    }

    private boolean canReadClusterConfig() {
        var clusterApi = createApiClient();
        try {
            clusterApi.getClusterConfiguration();
            return true;
        } catch (ApiException e) {
            return false;
        }
    }

    @NotNull
    private ClusterConfigurationApi createApiClient() {
        ApiClient client = Configuration.getDefaultApiClient();
        client.setBasePath(session.getNodeUrl());
        return new ClusterConfigurationApi(client);
    }
}
