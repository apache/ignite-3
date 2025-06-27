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

package org.apache.ignite.internal.topology;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toSet;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.network.ClusterNode;

/**
 * Test implementation of {@link LogicalTopologyService}.
 */
public class TestLogicalTopologyService implements LogicalTopologyService {
    private final ClusterService clusterService;

    private final UUID clusterId = UUID.randomUUID();

    public TestLogicalTopologyService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public void addEventListener(LogicalTopologyEventListener listener) {

    }

    @Override
    public void removeEventListener(LogicalTopologyEventListener listener) {

    }

    @Override
    public CompletableFuture<LogicalTopologySnapshot> logicalTopologyOnLeader() {
        return completedFuture(new LogicalTopologySnapshot(
                1,
                clusterService.topologyService().allMembers().stream().map(LogicalNode::new).collect(toSet()),
                clusterId
        ));
    }

    @Override
    public LogicalTopologySnapshot localLogicalTopology() {
        return new LogicalTopologySnapshot(
                1,
                clusterService.topologyService().allMembers().stream().map(LogicalNode::new).collect(toSet()),
                clusterId
        );
    }

    @Override
    public CompletableFuture<Set<ClusterNode>> validatedNodesOnLeader() {
        return completedFuture(Set.copyOf(clusterService.topologyService().allMembers()));
    }
}
