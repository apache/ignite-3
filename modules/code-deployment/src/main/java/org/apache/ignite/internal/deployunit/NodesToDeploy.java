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

package org.apache.ignite.internal.deployunit;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.deployunit.exception.InvalidNodesArgumentException;
import org.apache.ignite.internal.network.InternalClusterNode;

/**
 * Nodes for initial deploy.
 */
public class NodesToDeploy {
    /**
     * Direct nodes list.
     */
    private final List<String> nodesList;

    /**
     * Deploy nodes mode.
     */
    private final InitialDeployMode deployMode;

    public NodesToDeploy(List<String> nodesList) {
        this(null, nodesList);
    }

    public NodesToDeploy(InitialDeployMode deployMode) {
        this(deployMode, null);
    }

    private NodesToDeploy(InitialDeployMode deployMode, List<String> nodesList) {
        this.deployMode = deployMode;
        this.nodesList = nodesList;
    }

    /**
     * Returns a list of nodes for initial deployment.
     *
     * @param cmgManager Cluster management group.
     * @return Set of nodes for initial deployment.
     */
    public CompletableFuture<Set<String>> extractNodes(ClusterManagementGroupManager cmgManager) {
        return nodesList != null ? extractNodesFromList(cmgManager) : extractNodesFromMode(cmgManager);
    }

    private CompletableFuture<Set<String>> extractNodesFromMode(ClusterManagementGroupManager cmgManager) {
        switch (deployMode) {
            case ALL:
                return cmgManager.logicalTopology()
                        .thenApply(snapshot -> snapshot.nodes().stream()
                                .map(InternalClusterNode::name)
                                .collect(Collectors.toUnmodifiableSet()));
            case MAJORITY:
            default:
                return cmgManager.majority();
        }
    }

    /**
     * Gets a list of nodes for initial deployment. Always contains at least a majority of CMG nodes.
     *
     * @param cmgManager CMG manager.
     * @return Completed future with a set of consistent IDs, or a future, completed exceptionally with
     *         {@link InvalidNodesArgumentException} if any of the nodes are not present in the logical topology.
     */
    private CompletableFuture<Set<String>> extractNodesFromList(ClusterManagementGroupManager cmgManager) {
        return cmgManager.majority()
                .thenCompose(majority -> cmgManager.logicalTopology()
                        .thenApply(snapshot -> snapshot.nodes().stream()
                                .map(InternalClusterNode::name)
                                .collect(Collectors.toUnmodifiableSet()))
                        .thenApply(allNodes -> {
                            Set<String> result = new HashSet<>(majority);
                            for (String node : nodesList) {
                                if (!allNodes.contains(node)) {
                                    throw new InvalidNodesArgumentException(
                                            "Node \"" + node + "\" is not present in the logical topology"
                                    );
                                }
                                result.add(node);
                            }
                            return result;
                        })
                );
    }

    @Override
    public String toString() {
        return "NodesToDeploy{"
                + "nodesList=" + nodesList
                + ", deployMode=" + deployMode
                + '}';
    }
}
