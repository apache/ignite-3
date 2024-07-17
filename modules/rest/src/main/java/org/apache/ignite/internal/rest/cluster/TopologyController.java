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

package org.apache.ignite.internal.rest.cluster;

import static java.util.stream.Collectors.toList;

import io.micronaut.http.annotation.Controller;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.api.cluster.ClusterNode;
import org.apache.ignite.internal.rest.api.cluster.NetworkAddress;
import org.apache.ignite.internal.rest.api.cluster.NodeMetadata;
import org.apache.ignite.internal.rest.api.cluster.TopologyApi;

/**
 * Cluster topology endpoint implementation.
 */
@Controller("/management/v1/cluster/topology")
public class TopologyController implements TopologyApi, ResourceHolder {
    private TopologyService topologyService;

    private ClusterManagementGroupManager cmgManager;

    public TopologyController(TopologyService topologyService, ClusterManagementGroupManager cmgManager) {
        this.topologyService = topologyService;
        this.cmgManager = cmgManager;
    }

    /** {@inheritDoc} */
    @Override
    public Collection<ClusterNode> physicalTopology() {
        return toClusterNodeDtos(topologyService.allMembers());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<ClusterNode>> logicalTopology() {
        // TODO: IGNITE-18277 - return an object containing both nodes and topology version.

        return cmgManager.clusterState()
                .thenCompose(state -> cmgManager.logicalTopology())
                .thenApply(LogicalTopologySnapshot::nodes)
                .thenApply(TopologyController::toClusterNodeDtosFromLogicalNodes);
    }

    private static List<ClusterNode> toClusterNodeDtos(Collection<org.apache.ignite.network.ClusterNode> nodes) {
        return nodes.stream().map(TopologyController::toClusterNodeDto).collect(toList());
    }

    private static List<ClusterNode> toClusterNodeDtosFromLogicalNodes(Collection<LogicalNode> nodes) {
        return nodes.stream().map(TopologyController::toClusterNodeDto).collect(toList());
    }

    private static ClusterNode toClusterNodeDto(org.apache.ignite.network.ClusterNode node) {
        org.apache.ignite.network.NetworkAddress addr = node.address();

        var addrDto = new NetworkAddress(addr.host(), addr.port());
        return new ClusterNode(node.id(), node.name(), addrDto, toNodeMetadataDto(node.nodeMetadata()));
    }

    private static NodeMetadata toNodeMetadataDto(org.apache.ignite.network.NodeMetadata metadata) {
        if (metadata == null) {
            return null;
        }
        return new NodeMetadata(metadata.restHost(), metadata.httpPort(), metadata.httpsPort());
    }

    @Override
    public void cleanResources() {
        topologyService = null;
        cmgManager = null;
    }
}
