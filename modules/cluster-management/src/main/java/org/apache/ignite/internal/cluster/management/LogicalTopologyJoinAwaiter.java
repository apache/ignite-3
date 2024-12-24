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

package org.apache.ignite.internal.cluster.management;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.TopologyEventHandler;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.network.ClusterNode;

class LogicalTopologyJoinAwaiter {
    private static final IgniteLogger log = Loggers.forClass(LogicalTopologyJoinAwaiter.class);

    static CompletableFuture<Void> awaitLogicalTopologyToMatchPhysicalTopology(
            TopologyService physicalTopologyService,
            LogicalTopology logicalTopologyService
    ) {
        CopyOnWriteArraySet<UUID> physicalTopologyNodesId = physicalTopologyService.allMembers()
                .stream()
                .map(ClusterNode::id)
                .collect(Collectors.toCollection(CopyOnWriteArraySet::new));
        CopyOnWriteArraySet<UUID> logicalTopologyNodesId = logicalTopologyService.getLogicalTopology().nodes()
                .stream()
                .map(ClusterNode::id)
                .collect(Collectors.toCollection(CopyOnWriteArraySet::new));

        CompletableFuture<Void> result = new CompletableFuture<>();

        class TopologyListener implements TopologyEventHandler, LogicalTopologyEventListener {
            @Override
            public void onAppeared(ClusterNode member) {
                physicalTopologyNodesId.add(member.id());
                ensureTopologyMatch();
            }

            @Override
            public void onDisappeared(ClusterNode member) {
                physicalTopologyNodesId.remove(member.id());
                ensureTopologyMatch();
            }

            @Override
            public void onNodeJoined(LogicalNode joinedNode, LogicalTopologySnapshot newTopology) {
                logicalTopologyNodesId.add(joinedNode.id());
                ensureTopologyMatch();
            }

            @Override
            public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
                logicalTopologyNodesId.remove(leftNode.id());
                ensureTopologyMatch();
            }

            @Override
            public void onTopologyLeap(LogicalTopologySnapshot newTopology) {
                Set<UUID> newTopologyNodeIds = logicalTopologyService.getLogicalTopology().nodes()
                        .stream()
                        .map(ClusterNode::id)
                        .collect(Collectors.toSet());

                logicalTopologyNodesId.retainAll(newTopologyNodeIds);
                logicalTopologyNodesId.addAll(newTopologyNodeIds);

                ensureTopologyMatch();
            }

            private void ensureTopologyMatch() {
                log.debug("New physical topology: {}, new logical topology: {}", physicalTopologyNodesId, logicalTopologyNodesId);
                if (physicalTopologyNodesId.equals(logicalTopologyNodesId)) {
                    result.complete(null);
                    physicalTopologyService.removeEventHandler(this);
                    logicalTopologyService.removeEventListener(this);
                }
            }
        }

        if (physicalTopologyNodesId.equals(logicalTopologyNodesId)) {
            // No need to wait for topology changes.
            result.complete(null);
        } else {
            // Wait for logical topology to match physical topology.
            TopologyListener topologyListener = new TopologyListener();
            physicalTopologyService.addEventHandler(topologyListener);
            logicalTopologyService.addEventListener(topologyListener);
        }

        return result;
    }
}
