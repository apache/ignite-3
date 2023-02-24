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

package org.apache.ignite.internal.cluster.management.rest;

import static java.util.stream.Collectors.toList;

import io.micronaut.http.annotation.Controller;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.rest.api.cluster.ClusterNodeDto;
import org.apache.ignite.internal.rest.api.cluster.NetworkAddressDto;
import org.apache.ignite.internal.rest.api.cluster.NodeMetadataDto;
import org.apache.ignite.internal.rest.api.cluster.TopologyApi;
import org.apache.ignite.internal.rest.exception.ClusterNotInitializedException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeMetadata;
import org.apache.ignite.network.TopologyService;

/**
 * Cluster topology endpoint implementation.
 */
@Controller("/management/v1/cluster/topology")
public class TopologyController implements TopologyApi {
    private final TopologyService topologyService;

    private final ClusterManagementGroupManager cmgManager;

    public TopologyController(TopologyService topologyService, ClusterManagementGroupManager cmgManager) {
        this.topologyService = topologyService;
        this.cmgManager = cmgManager;
    }

    /** {@inheritDoc} */
    @Override
    public Collection<ClusterNodeDto> physicalTopology() {
        return toClusterNodeDtos(topologyService.allMembers());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<ClusterNodeDto>> logicalTopology() {
        // TODO: IGNITE-18277 - return an object containing both nodes and topology version.

        return cmgManager.clusterState()
                .thenCompose(state -> {
                    if (state == null) {
                        throw new ClusterNotInitializedException();
                    }

                    return cmgManager.logicalTopology();
                })
                .thenApply(LogicalTopologySnapshot::nodes)
                .thenApply(TopologyController::toClusterNodeDtos);
    }

    private static List<ClusterNodeDto> toClusterNodeDtos(Collection<ClusterNode> nodes) {
        return nodes.stream().map(TopologyController::toClusterNodeDto).collect(toList());
    }

    private static ClusterNodeDto toClusterNodeDto(ClusterNode node) {
        NetworkAddress addr = node.address();

        var addrDto = new NetworkAddressDto(addr.host(), addr.port());
        return new ClusterNodeDto(node.id(), node.name(), addrDto, toNodeMetadataDto(node.nodeMetadata()));
    }

    private static NodeMetadataDto toNodeMetadataDto(NodeMetadata metadata) {
        if (metadata == null) {
            return null;
        }
        return new NodeMetadataDto(metadata.restHost(), metadata.httpPort(), metadata.httpsPort());
    }
}
