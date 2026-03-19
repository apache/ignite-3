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
import org.apache.ignite.internal.cli.call.cluster.status.ClusterStatus.ClusterStatusBuilder;
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
import org.apache.ignite.rest.client.model.ClusterState;

/**
 * Call to get cluster status.
 */
@Singleton
public class ClusterStatusCall implements Call<UrlCallInput, ClusterStatus> {

    private final PhysicalTopologyCall physicalTopologyCall;

    private final ApiClientFactory clientFactory;

    public ClusterStatusCall(PhysicalTopologyCall physicalTopologyCall, ApiClientFactory clientFactory) {
        this.physicalTopologyCall = physicalTopologyCall;
        this.clientFactory = clientFactory;
    }

    @Override
    public CallOutput<ClusterStatus> execute(UrlCallInput input) {
        ClusterStatusBuilder clusterStatusBuilder = ClusterStatus.builder();
        String clusterUrl = input.getUrl();
        try {
            ClusterState clusterState = fetchClusterState(clusterUrl);
            clusterStatusBuilder
                    .nodeCount(fetchNumberOfAllNodes(input))
                    .initialized(true)
                    .name(clusterState.getClusterTag().getClusterName())
                    .metadataStorageNodes(clusterState.getMsNodes())
                    .cmgNodes(clusterState.getCmgNodes());
        } catch (ApiException e) {
            if (e.getCode() == 409) { // CONFLICT means the cluster is not initialized yet
                clusterStatusBuilder.initialized(false).nodeCount(fetchNumberOfAllNodes(input));
            } else {
                return DefaultCallOutput.failure(new IgniteCliApiException(e, clusterUrl));
            }
        } catch (IllegalArgumentException e) {
            return DefaultCallOutput.failure(new IgniteCliApiException(e, clusterUrl));
        }

        return DefaultCallOutput.success(clusterStatusBuilder.build());
    }

    private int fetchNumberOfAllNodes(UrlCallInput input) {
        List<ClusterNode> body = physicalTopologyCall.execute(input).body();
        if (body == null) {
            return -1;
        }
        return body.size();
    }

    private ClusterState fetchClusterState(String url) throws ApiException {
        return new ClusterManagementApi(clientFactory.getClient(url)).clusterState();
    }
}
