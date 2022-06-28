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
import org.apache.ignite.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.cli.core.repl.Session;
import org.apache.ignite.cli.deprecated.CliPathsConfigLoader;
import org.apache.ignite.cli.deprecated.IgnitePaths;
import org.apache.ignite.cli.deprecated.builtins.node.NodeManager;
import org.apache.ignite.rest.client.api.ClusterManagementApi;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.ClusterState;

/**
 * Call to get cluster status.
 */
@Singleton
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
        try {
            ClusterState clusterState = fetchClusterState();
            return DefaultCallOutput.success(
                    Status.builder()
                            .connected(session.isConnectedToNode())
                            .connectedNodeUrl(session.getNodeUrl())
                            .initialized(true)
                            .name(clusterState.getClusterTag().getClusterName())
                            .cmgNodes(clusterState.getCmgNodes())
                            .nodeCount(nodeManager.getRunningNodes(paths.logDir, paths.cliPidsDir()).size())
                            .build()
            );
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                return DefaultCallOutput.success(
                        Status.builder()
                                .connected(session.isConnectedToNode())
                                .connectedNodeUrl(session.getNodeUrl())
                                .initialized(false)
                                .nodeCount(nodeManager.getRunningNodes(paths.logDir, paths.cliPidsDir()).size())
                                .build());
            }

            return DefaultCallOutput.failure(new IgniteCliApiException(e, ""));
        } catch (IllegalArgumentException e) {
            return DefaultCallOutput.success(
                    Status.builder()
                            .connected(session.isConnectedToNode())
                            .connectedNodeUrl(session.getNodeUrl())
                            .initialized(false)
                            .nodeCount(nodeManager.getRunningNodes(paths.logDir, paths.cliPidsDir()).size())
                            .build());
        }
    }

    private ClusterState fetchClusterState() throws ApiException {
        return new ClusterManagementApi(new ApiClient().setBasePath(session.getNodeUrl())).clusterState();
    }
}
