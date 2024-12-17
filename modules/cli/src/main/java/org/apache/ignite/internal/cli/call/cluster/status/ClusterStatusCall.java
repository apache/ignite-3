/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cli.call.cluster.status;

import jakarta.inject.Singleton;
import java.util.List;
import org.apache.ignite.internal.cli.call.cluster.status.ClusterStateOutput.ClusterStateBuilder;
import org.apache.ignite.internal.cli.call.cluster.topology.PhysicalTopologyCall;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.rest.client.api.ClusterManagementApi;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.ClusterNode;
import org.apache.ignite.rest.client.model.ClusterStateDto;

/**
 * Call to get cluster status.
 */
@Singleton
public class ClusterStatusCall implements Call<UrlCallInput, ClusterStateOutput> {
    /**
     * We need to overlap timeout from raft client.
     * We can't determine timeout value because it's configurable for each node
     * And moreover retry counter can be increased.
     * See {@link org.apache.ignite.internal.rest.cluster.ClusterManagementController}
     * {@link org.apache.ignite.internal.raft.configuration.RaftConfigurationSchema}.
     */
    private static final int READ_TIMEOUT = 40_000;

    private final PhysicalTopologyCall physicalTopologyCall;

    private final ApiClientFactory clientFactory;

    public ClusterStatusCall(PhysicalTopologyCall physicalTopologyCall, ApiClientFactory clientFactory) {
        this.physicalTopologyCall = physicalTopologyCall;
        this.clientFactory = clientFactory;
    }

    @Override
    public CallOutput<ClusterStateOutput> execute(UrlCallInput input) {
        ClusterStateBuilder clusterStateBuilder = ClusterStateOutput.builder();
        String clusterUrl = input.getUrl();
        try {
            ClusterStateDto clusterState = fetchClusterState(clusterUrl);
            clusterStateBuilder
                    .nodeCount(fetchNumberOfAllNodes(input))
                    .initialized(true)
                    .name(clusterState.getClusterTag().getClusterName())
                    .metastoreStatus(clusterState.getMetastoreStatus())
                    .cmgStatus(clusterState.getCmgStatus());
        } catch (ApiException e) {
            if (e.getCode() == 409) { // CONFLICT means the cluster is not initialized yet
                clusterStateBuilder.initialized(false).nodeCount(fetchNumberOfAllNodes(input));
            } else {
                return DefaultCallOutput.failure(new IgniteCliApiException(e, clusterUrl));
            }
        } catch (IllegalArgumentException e) {
            return DefaultCallOutput.failure(new IgniteCliApiException(e, clusterUrl));
        }

        return DefaultCallOutput.success(clusterStateBuilder.build());
    }

    private int fetchNumberOfAllNodes(UrlCallInput input) {
        List<ClusterNode> body = physicalTopologyCall.execute(input).body();
        if (body == null) {
            return -1;
        }
        return body.size();
    }

    private ClusterStateDto fetchClusterState(String url) throws ApiException {
        return new ClusterManagementApi(clientFactory.getClient(url)
                .setReadTimeout(READ_TIMEOUT))
                .clusterState();
    }
}
