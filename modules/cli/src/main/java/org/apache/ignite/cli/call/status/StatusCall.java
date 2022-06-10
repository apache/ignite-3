/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.call.status;

import jakarta.inject.Singleton;
import java.net.ConnectException;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.core.call.StatusCallInput;
import org.apache.ignite.cli.core.exception.CommandExecutionException;
import org.apache.ignite.cli.deprecated.CliPathsConfigLoader;
import org.apache.ignite.cli.deprecated.IgnitePaths;
import org.apache.ignite.cli.deprecated.builtins.node.NodeManager;
import org.apache.ignite.rest.client.api.ClusterConfigurationApi;
import org.apache.ignite.rest.client.api.NodeConfigurationApi;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.invoker.Configuration;

/**
 * Call to get cluster status.
 */
@Singleton
//TODO: https://issues.apache.org/jira/browse/IGNITE-17093
public class StatusCall implements Call<StatusCallInput, Status> {

    private final NodeManager nodeManager;

    private final CliPathsConfigLoader cliPathsCfgLdr;

    /**
     * Default constructor.
     */
    public StatusCall(NodeManager nodeManager, CliPathsConfigLoader cliPathsCfgLdr) {
        this.nodeManager = nodeManager;
        this.cliPathsCfgLdr = cliPathsCfgLdr;
    }

    @Override
    public CallOutput<Status> execute(StatusCallInput input) {
        IgnitePaths paths = cliPathsCfgLdr.loadIgnitePathsOrThrowError();
        String connected = null;
        try {
            connected = createNodeApi(input).getNodeConfiguration();
        } catch (ApiException e) {
            if (e.getCause() instanceof ConnectException) {
                return DefaultCallOutput.failure(new CommandExecutionException("status", "cannot connect to " + input.getClusterUrl()));
            } else {
                return DefaultCallOutput.failure(e);
            }
        }
        return DefaultCallOutput.success(
                Status.builder()
                        .connected(connected != null)
                        .connectedNodeUrl(input.getClusterUrl())
                        .initialized(connected != null && canReadClusterConfig(input))
                        .nodeCount(nodeManager.getRunningNodes(paths.logDir, paths.cliPidsDir()).size())
                        .build()
        );
    }

    private boolean canReadClusterConfig(StatusCallInput input) {
        var clusterApi = createClusterApi(input);
        try {
            clusterApi.getClusterConfiguration();
            return true;
        } catch (ApiException e) {
            return false;
        }
    }

    private ClusterConfigurationApi createClusterApi(StatusCallInput input) {
        ApiClient client = Configuration.getDefaultApiClient();
        client.setBasePath(input.getClusterUrl());
        return new ClusterConfigurationApi(client);
    }

    private NodeConfigurationApi createNodeApi(StatusCallInput input) {
        ApiClient client = Configuration.getDefaultApiClient();
        client.setBasePath(input.getClusterUrl());
        return new NodeConfigurationApi(client);
    }
}
