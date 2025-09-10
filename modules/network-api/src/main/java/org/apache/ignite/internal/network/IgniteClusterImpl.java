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

package org.apache.ignite.internal.network;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.IgniteCluster;
import org.jetbrains.annotations.Nullable;

/**
 * Cluster implementation.
 */
public class IgniteClusterImpl implements IgniteCluster {
    public final TopologyService topologyService;

    public final ClusterIdSupplier clusterIdSupplier;

    public IgniteClusterImpl(TopologyService topologyService, ClusterIdSupplier clusterIdSupplier) {
        this.topologyService = topologyService;
        this.clusterIdSupplier = clusterIdSupplier;
    }

    @Override
    public UUID id() {
        @Nullable UUID ret = clusterIdSupplier.clusterId();
        assert ret != null : "clusterId not available";
        return ret;
    }

    @Override
    public Collection<ClusterNode> nodes() {
        return toPublicClusterNodes(topologyService.logicalTopologyMembers());
    }

    private static List<ClusterNode> toPublicClusterNodes(Collection<InternalClusterNode> nodes) {
        return nodes.stream()
                .map(InternalClusterNode::toPublicNode)
                .collect(Collectors.toList());
    }

    @Override
    public CompletableFuture<Collection<ClusterNode>> nodesAsync() {
        return completedFuture(toPublicClusterNodes(topologyService.allMembers()));
    }

    @Override
    public @Nullable ClusterNode localNode() {
        return topologyService.localMember().toPublicNode();
    }
}
