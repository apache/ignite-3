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

package org.apache.ignite.internal.cluster.management.raft;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;

import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.ClusterTag;
import org.apache.ignite.internal.cluster.management.raft.commands.InitCmgStateCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinReadyCommand;
import org.apache.ignite.internal.cluster.management.raft.responses.ValidationErrorResponse;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Class responsible for validating cluster nodes.
 *
 * <p>If a node passes the validation successfully, a unique validation token is issued which exists for a specific period of time.
 * After the node finishes local recovery procedures, it sends a {@link JoinReadyCommand} containing the validation token. If the local
 * token and the received token match, the node will be added to the logical topology and the token will be invalidated.
 */
public class ValidationManager {
    protected final ClusterStateStorageManager storageManager;

    protected final LogicalTopology logicalTopology;

    public ValidationManager(ClusterStateStorageManager storageManager, LogicalTopology logicalTopology) {
        this.storageManager = storageManager;
        this.logicalTopology = logicalTopology;
    }

    /**
     * Validates a given {@code state} against the {@code nodeState} received from an {@link InitCmgStateCommand}.
     */
    static ValidationResult validateState(ClusterState state, ClusterState nodeState) {
        if (!state.cmgNodes().equals(nodeState.cmgNodes())) {
            return ValidationResult.errorResult(String.format(
                    "CMG node names do not match. CMG nodes: %s, nodes stored in CMG: %s",
                    nodeState.cmgNodes(), state.cmgNodes()
            ));
        }

        if (!state.metaStorageNodes().equals(nodeState.metaStorageNodes())) {
            return ValidationResult.errorResult(String.format(
                    "MetaStorage node names do not match. MetaStorage nodes: %s, nodes stored in CMG: %s",
                    nodeState.metaStorageNodes(), state.metaStorageNodes()
            ));
        }

        if (!state.igniteVersion().equals(nodeState.igniteVersion())) {
            return ValidationResult.errorResult(String.format(
                    "Ignite versions do not match. Version: %s, version stored in CMG: %s",
                    nodeState.igniteVersion(), state.igniteVersion()
            ));
        }

        // We only compare cluster names here, because multiple nodes will try to generate and set the cluster ID, so that they will never
        // actually match.
        if (!state.clusterTag().clusterName().equals(nodeState.clusterTag().clusterName())) {
            return ValidationResult.errorResult(String.format(
                    "Cluster names do not match. Cluster name: %s, cluster name stored in CMG: %s",
                    nodeState.clusterTag().clusterName(), state.clusterTag().clusterName()
            ));
        }

        return ValidationResult.successfulResult();
    }

    /**
     * Validates a given node and saves it in the set of validated nodes.
     *
     * @param state Cluster state.
     * @param node Node that wishes to join the logical topology.
     * @param clusterTag Cluster tag.
     * @return A {@link ValidationErrorResponse} with validation results.
     */
    protected ValidationResult validateNode(
            @Nullable ClusterState state,
            LogicalNode node,
            ClusterTag clusterTag
    ) {
        if (isNodeValidated(node)) {
            return ValidationResult.successfulResult();
        } else if (state == null) {
            return ValidationResult.errorResult("Cluster has not been initialized yet");
        } else if (!state.clusterTag().equals(clusterTag)) {
            return ValidationResult.errorResult(String.format(
                    "Cluster tags do not match. Cluster tag: %s, cluster tag stored in CMG: %s",
                    clusterTag, state.clusterTag()
            ));
        } else if (!isColocationEnabledMatched(isColocationEnabled(node))) {
            return ValidationResult.configErrorResult(String.format(
                    "Colocation enabled mode does not match. Joining node colocation mode is: %s, cluster colocation mode is: %s",
                    isColocationEnabled(node),
                    isColocationEnabled(logicalTopology.getLogicalTopology().nodes().iterator().next())
            ));
        } else {
            putValidatedNode(node);

            return ValidationResult.successfulResult();
        }
    }

    private static boolean isColocationEnabled(LogicalNode node) {
        return Boolean.parseBoolean(node.systemAttributes().get(COLOCATION_FEATURE_FLAG));
    }

    private boolean isColocationEnabledMatched(boolean joiningNodeColocationEnabled) {
        Set<LogicalNode> logicalTopologyNodes = logicalTopology.getLogicalTopology().nodes();

        return logicalTopologyNodes.isEmpty()
                || isColocationEnabled(logicalTopologyNodes.iterator().next()) == joiningNodeColocationEnabled;
    }

    boolean isNodeValidated(LogicalNode node) {
        return storageManager.isNodeValidated(node) || logicalTopology.isNodeInLogicalTopology(node);
    }

    void putValidatedNode(LogicalNode node) {
        storageManager.putValidatedNode(node);

        logicalTopology.onNodeValidated(node);
    }

    void removeValidatedNodes(Collection<LogicalNode> nodes) {
        Set<UUID> validatedNodeIds = storageManager.getValidatedNodes().stream()
                .map(InternalClusterNode::id)
                .collect(toSet());

        // Using a sorted stream to have a stable notification order.
        nodes.stream()
                .filter(node -> validatedNodeIds.contains(node.id()))
                .sorted(Comparator.comparing(InternalClusterNode::id))
                .forEach(node -> {
                    storageManager.removeValidatedNode(node);

                    logicalTopology.onNodeInvalidated(node);
                });
    }

    /**
     * Removes the node from the list of validated nodes thus completing the validation procedure.
     *
     * @param node Node that wishes to join the logical topology.
     * @return A {@link ValidationErrorResponse} with validation results.
     */
    protected ValidationResult completeValidation(LogicalNode node) {
        // Remove all other versions of this node, if they were validated at some point, but not removed from the physical topology.
        storageManager.getValidatedNodes().stream()
                .filter(n -> n.name().equals(node.name()) && !n.id().equals(node.id()))
                .sorted(Comparator.comparing(InternalClusterNode::id))
                .forEach(nodeVersion -> {
                    storageManager.removeValidatedNode(nodeVersion);

                    logicalTopology.onNodeInvalidated(nodeVersion);
                });

        storageManager.removeValidatedNode(node);

        return ValidationResult.successfulResult();
    }
}
