/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinReadyCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinRequestCommand;
import org.apache.ignite.internal.cluster.management.raft.responses.JoinDeniedResponse;
import org.apache.ignite.internal.cluster.management.raft.responses.NodeValidatedResponse;
import org.apache.ignite.internal.cluster.management.validation.NodeValidator;
import org.apache.ignite.internal.cluster.management.validation.ValidationError;
import org.apache.ignite.internal.cluster.management.validation.ValidationInfo;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Class responsible for managing validation tokens.
 *
 * <p>If a node passes the validation successfully, a unique validation token is issued which exists for a specific period of time.
 * After the node finishes local recovery procedures, it sends a {@link JoinReadyCommand} containing the validation
 * token. If the local token and the received token match, the node will be added to the logical topology and the token will be invalidated.
 */
class ValidationTokenManager implements AutoCloseable {
    private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("node-validator"));

    private final RaftStorageManager storage;

    /**
     * Map for storing tasks, submitted to the {@link #executor}, so that it is possible to cancel them.
     */
    private final Map<String, Future<?>> cleanupFutures = new ConcurrentHashMap<>();

    ValidationTokenManager(RaftStorageManager storage) {
        this.storage = storage;

        // schedule removal of possibly stale tokens in case the leader has changed or the node has been restarted
        storage.getValidatedNodeIds().forEach(this::scheduleValidationTokenRemoval);
    }

    /**
     * Validates a given node and issues a validation token.
     *
     * @param command Command sent by the node that intends to join the cluster.
     * @return {@link NodeValidatedResponse} in case of successful validation or a {@link JoinDeniedResponse} otherwise.
     */
    Serializable validateNode(JoinRequestCommand command) {
        ClusterState state = storage.getClusterState();

        if (state == null) {
            return new JoinDeniedResponse("Cluster has not been initialized yet");
        }

        var validationInfo = new ValidationInfo(command.igniteVersion(), command.clusterTag());

        ValidationError validationError = NodeValidator.validateNode(state, validationInfo);

        if (validationError != null) {
            return new JoinDeniedResponse(validationError.rejectReason());
        }

        UUID validationToken = UUID.randomUUID();

        ClusterNode validatedNode = command.node();

        storage.putValidationToken(validatedNode, validationToken);

        scheduleValidationTokenRemoval(validatedNode.id());

        return new NodeValidatedResponse(validationToken);
    }

    /**
     * Checks and invalidates the given node's validation token thus completing the validation procedure.
     *
     * @param command Command sent by the node that wishes to join the logical topology.
     * @return {@code null} if the tokens match or {@link JoinDeniedResponse} otherwise.
     */
    @Nullable
    Serializable completeValidation(JoinReadyCommand command) {
        UUID validationToken = storage.getValidationToken(command.node());

        String nodeId = command.node().id();

        if (validationToken == null) {
            return new JoinDeniedResponse(String.format("Node \"%s\" has not yet passed the validation step", nodeId));
        } else if (!validationToken.equals(command.validationToken())) {
            return new JoinDeniedResponse("Incorrect validation token");
        }

        Future<?> cleanupFuture = cleanupFutures.remove(nodeId);

        if (cleanupFuture != null) {
            cleanupFuture.cancel(false);
        }

        storage.removeValidationToken(nodeId);

        return null;
    }

    private void scheduleValidationTokenRemoval(String nodeId) {
        // TODO: delay should be configurable, see https://issues.apache.org/jira/browse/IGNITE-16785
        Future<?> future = executor.schedule(() -> {
            cleanupFutures.remove(nodeId);

            storage.removeValidationToken(nodeId);
        }, 1, TimeUnit.HOURS);

        cleanupFutures.put(nodeId, future);
    }

    @Override
    public void close() {
        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);

        cleanupFutures.clear();
    }
}
