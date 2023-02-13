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

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.ClusterTag;
import org.apache.ignite.internal.cluster.management.raft.commands.InitCmgStateCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinReadyCommand;
import org.apache.ignite.internal.cluster.management.raft.responses.ValidationErrorResponse;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Class responsible for validating cluster nodes.
 *
 * <p>If a node passes the validation successfully, a unique validation token is issued which exists for a specific period of time.
 * After the node finishes local recovery procedures, it sends a {@link JoinReadyCommand} containing the validation token. If the local
 * token and the received token match, the node will be added to the logical topology and the token will be invalidated.
 */
class ValidationManager {
    private final RaftStorageManager storage;

    private final LogicalTopology logicalTopology;

    ValidationManager(RaftStorageManager storage, LogicalTopology logicalTopology) {
        this.storage = storage;
        this.logicalTopology = logicalTopology;
    }

    /**
     * Validates a given {@code state} against the {@code nodeState} received from an {@link InitCmgStateCommand}.
     */
    static ValidationResult validateState(@Nullable ClusterState state, ClusterNode node, ClusterState nodeState) {
        if (state == null) {
            return ValidationResult.errorResult("Cluster has not been initialized yet");
        }

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
     * Validates a given node and issues a validation token.
     *
     * @return {@code null} in case of successful validation or a {@link ValidationErrorResponse} otherwise.
     */
    ValidationResult validateNode(
            @Nullable ClusterState state,
            ClusterNode node,
            IgniteProductVersion version,
            ClusterTag clusterTag
    ) {
        if (isNodeValidated(node)) {
            return ValidationResult.successfulResult();
        } else if (state == null) {
            return ValidationResult.errorResult("Cluster has not been initialized yet");
        } else if (!state.igniteVersion().equals(version)) {
            return ValidationResult.errorResult(String.format(
                    "Ignite versions do not match. Version: %s, version stored in CMG: %s",
                    version, state.igniteVersion()
            ));
        } else if (!state.clusterTag().equals(clusterTag)) {
            return ValidationResult.errorResult(String.format(
                    "Cluster tags do not match. Cluster tag: %s, cluster tag stored in CMG: %s",
                    clusterTag, state.clusterTag()
            ));
        } else {
            putValidatedNode(node);

            return ValidationResult.successfulResult();
        }
    }

    boolean isNodeValidated(ClusterNode node) {
        return storage.isNodeValidated(node) || logicalTopology.isNodeInLogicalTopology(node);
    }

    void putValidatedNode(ClusterNode node) {
        storage.putValidatedNode(node);

        logicalTopology.onNodeValidated(node);
    }

    void removeValidatedNodes(Collection<ClusterNode> nodes) {
        Set<String> validatedNodeIds = storage.getValidatedNodes().stream()
                .map(ClusterNode::id)
                // Using a sorted collection to have a stable notification order.
                .collect(Collectors.toCollection(TreeSet::new));

        nodes.forEach(node -> {
            if (validatedNodeIds.contains(node.id())) {
                storage.removeValidatedNode(node);

                logicalTopology.onNodeInvalidated(node);
            }
        });
    }

    /**
     * Removes the node from the list of validated nodes thus completing the validation procedure.
     *
     * @param node Node that wishes to join the logical topology.
     */
    void completeValidation(ClusterNode node) {
        storage.removeValidatedNode(node);
    }
}
