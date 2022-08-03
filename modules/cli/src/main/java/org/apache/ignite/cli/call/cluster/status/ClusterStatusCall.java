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

package org.apache.ignite.cli.call.cluster.status;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.call.cluster.status.ClusterStatus.ClusterStatusBuilder;
import org.apache.ignite.cli.call.cluster.topology.PhysicalTopologyCall;
import org.apache.ignite.cli.call.cluster.topology.TopologyCallInput;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.core.call.StatusCallInput;
import org.apache.ignite.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.rest.client.api.ClusterManagementApi;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.ClusterState;

/**
 * Call to get cluster status.
 */
@Singleton
public class ClusterStatusCall implements Call<StatusCallInput, ClusterStatus> {

    private final PhysicalTopologyCall physicalTopologyCall;

    public ClusterStatusCall(PhysicalTopologyCall physicalTopologyCall) {
        this.physicalTopologyCall = physicalTopologyCall;
    }

    @Override
    public CallOutput<ClusterStatus> execute(StatusCallInput input) {
        ClusterStatusBuilder clusterStatusBuilder = ClusterStatus.builder();
        try {
            ClusterState clusterState = fetchClusterState(input.getClusterUrl());
            clusterStatusBuilder
                    .nodeCount(fetchNumberOfAllNodes(input.getClusterUrl()))
                    .initialized(true)
                    .name(clusterState.getClusterTag().getClusterName())
                    .metadataStorageNodes(clusterState.getMsNodes())
                    .cmgNodes(clusterState.getCmgNodes());
        } catch (ApiException e) {
            if (e.getCode() == 404) { // NOT_FOUND means the cluster is not initialized yet
                clusterStatusBuilder.initialized(false).nodeCount(fetchNumberOfAllNodes(input.getClusterUrl()));
            } else {
                return DefaultCallOutput.failure(new IgniteCliApiException(e, input.getClusterUrl()));
            }
        } catch (IllegalArgumentException e) {
            return DefaultCallOutput.failure(new IgniteCliApiException(e, input.getClusterUrl()));
        }

        return DefaultCallOutput.success(clusterStatusBuilder.build());
    }

    private int fetchNumberOfAllNodes(String url) {
        return physicalTopologyCall.execute(TopologyCallInput.builder().clusterUrl(url).build()).body().size();
    }

    private ClusterState fetchClusterState(String url) throws ApiException {
        return new ClusterManagementApi(new ApiClient().setBasePath(url)).clusterState();
    }
}
