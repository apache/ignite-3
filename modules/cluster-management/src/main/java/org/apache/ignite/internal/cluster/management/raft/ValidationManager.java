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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.ClusterTag;
import org.apache.ignite.internal.cluster.management.raft.commands.InitCmgStateCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinReadyCommand;
import org.apache.ignite.internal.cluster.management.raft.responses.ValidationErrorResponse;
import org.apache.ignite.internal.cluster.management.topology.InternalLogicalTopologyService;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Class responsible for validating cluster nodes.
 *
 * <p>If a node passes the validation successfully, a unique validation token is issued which exists for a specific period of time.
 * After the node finishes local recovery procedures, it sends a {@link JoinReadyCommand} containing the validation
 * token. If the local token and the received token match, the node will be added to the logical topology and the token will be invalidated.
 */
class ValidationManager implements ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(CmgRaftGroupListener.class);

    private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("node-validator", LOG));

    private final RaftStorageManager storage;

    private final InternalLogicalTopologyService logicalTopologyService;

    /**
     * Map for storing tasks, submitted to the {@link #executor}, so that it is possible to cancel them.
     */
    private final Map<String, Future<?>> cleanupFutures = new ConcurrentHashMap<>();

    ValidationManager(RaftStorageManager storage, InternalLogicalTopologyService logicalTopologyService) {
        this.storage = storage;
        this.logicalTopologyService = logicalTopologyService;

        // Schedule removal of possibly stale node IDs in case the leader has changed or the node has been restarted.
        storage.getValidatedNodeIds().forEach(this::scheduleValidatedNodeRemoval);
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
        return storage.isNodeValidated(node.id()) || logicalTopologyService.isNodeInLogicalTopology(node);
    }

    /**
     * Checks and removes the node from the list of validated nodes thus completing the validation procedure.
     *
     * @param node Node that wishes to join the logical topology.
     */
    void completeValidation(ClusterNode node) {
        Future<?> cleanupFuture = cleanupFutures.remove(node.id());

        if (cleanupFuture != null) {
            cleanupFuture.cancel(false);
        }

        storage.removeValidatedNode(node.id());
    }

    private void putValidatedNode(ClusterNode node) {
        storage.putValidatedNode(node.id());

        scheduleValidatedNodeRemoval(node.id());
    }

    private void scheduleValidatedNodeRemoval(String nodeId) {
        // TODO: delay should be configurable, see https://issues.apache.org/jira/browse/IGNITE-16785
        Future<?> future = executor.schedule(() -> {
            LOG.info("Removing node from the list of validated nodes since no JoinReady requests have been received [node={}]", nodeId);

            cleanupFutures.remove(nodeId);

            storage.removeValidatedNode(nodeId);
        }, 1, TimeUnit.HOURS);

        cleanupFutures.put(nodeId, future);
    }

    @Override
    public void close() {
        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);

        cleanupFutures.clear();
    }
}
