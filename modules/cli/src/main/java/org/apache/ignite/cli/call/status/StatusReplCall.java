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
//TODO: https://issues.apache.org/jira/browse/IGNITE-17093
public class StatusReplCall implements Call<EmptyCallInput, Status> {

    private final NodeManager nodeManager;

    private final CliPathsConfigLoader cliPathsCfgLdr;

    private final Session session;

    /**
     * Default constructor.
     */
    public StatusReplCall(NodeManager nodeManager, CliPathsConfigLoader cliPathsCfgLdr, Session session) {
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
                        .build()
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
