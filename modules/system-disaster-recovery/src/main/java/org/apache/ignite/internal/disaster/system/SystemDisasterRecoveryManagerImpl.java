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

package org.apache.ignite.internal.disaster.system;

import static java.util.Objects.requireNonNullElse;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.util.CompletableFutures.allOf;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.network.messages.SuccessResponseMessage;
import org.apache.ignite.internal.disaster.system.message.ResetClusterMessage;
import org.apache.ignite.internal.disaster.system.message.SystemDisasterRecoveryMessageGroup;
import org.apache.ignite.internal.disaster.system.message.SystemDisasterRecoveryMessagesFactory;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.network.ClusterNode;

/**
 * Implementation of {@link SystemDisasterRecoveryManager}.
 */
public class SystemDisasterRecoveryManagerImpl implements SystemDisasterRecoveryManager, IgniteComponent {
    private final String thisNodeName;
    private final TopologyService topologyService;
    private final MessagingService messagingService;
    private final ServerRestarter restarter;

    private final SystemDisasterRecoveryMessagesFactory messagesFactory = new SystemDisasterRecoveryMessagesFactory();
    private static final CmgMessagesFactory cmgMessagesFactory = new CmgMessagesFactory();

    private final SystemDisasterRecoveryStorage storage;

    /** This executor spawns a thread per task and should only be used for very rare tasks. */
    private final Executor restartExecutor;

    /** Constructor. */
    public SystemDisasterRecoveryManagerImpl(
            String thisNodeName,
            TopologyService topologyService,
            MessagingService messagingService,
            VaultManager vaultManager,
            ServerRestarter restarter
    ) {
        this.thisNodeName = thisNodeName;
        this.topologyService = topologyService;
        this.messagingService = messagingService;
        this.restarter = restarter;

        storage = new SystemDisasterRecoveryStorage(vaultManager);
        restartExecutor = new ThreadPerTaskExecutor(thisNodeName + "-restart-");
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        messagingService.addMessageHandler(SystemDisasterRecoveryMessageGroup.class, (message, sender, correlationId) -> {
            if (message instanceof ResetClusterMessage) {
                assert correlationId != null;
                handleResetClusterMessage((ResetClusterMessage) message, sender, correlationId);
            }
        });

        return nullCompletedFuture();
    }

    private void handleResetClusterMessage(ResetClusterMessage message, ClusterNode sender, long correlationId) {
        restartExecutor.execute(() -> {
            storage.saveResetClusterMessage(message);

            messagingService.respond(sender, successResponseMessage(), correlationId)
                    .thenRunAsync(() -> {
                        if (!thisNodeName.equals(message.conductor())) {
                            restarter.initiateRestart();
                        }
                    }, restartExecutor);
        });
    }

    private static SuccessResponseMessage successResponseMessage() {
        return cmgMessagesFactory.successResponseMessage().build();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }

    @Override
    public void saveClusterState(ClusterState clusterState) {
        storage.saveClusterState(clusterState);
    }

    @Override
    public void markNodeInitialized() {
        storage.markNodeInitialized();
    }

    @Override
    public CompletableFuture<Void> resetCluster(List<String> proposedCmgConsistentIds) {
        try {
            return doResetCluster(proposedCmgConsistentIds);
        } catch (ClusterResetException e) {
            return failedFuture(e);
        }
    }

    private CompletableFuture<Void> doResetCluster(List<String> proposedCmgConsistentIds) {
        ensureNoRepetitions(proposedCmgConsistentIds);
        ensureContainsThisNodeName(proposedCmgConsistentIds);

        Collection<ClusterNode> nodesInTopology = topologyService.allMembers();
        ensureAllProposedCmgNodesAreInTopology(proposedCmgConsistentIds, nodesInTopology);

        ensureNodeIsInitialized();
        ClusterState clusterState = ensureClusterStateIsPresent();

        ResetClusterMessage message = buildResetClusterMessage(
                proposedCmgConsistentIds,
                clusterState
        );

        Map<String, CompletableFuture<NetworkMessage>> responseFutures = sendResetClusterMessageTo(
                nodesInTopology,
                message
        );

        return allOf(responseFutures.values())
                .handleAsync((res, ex) -> {
                    // We ignore upstream exceptions on purpose.

                    if (isMajorityOfCmgAreSuccesses(proposedCmgConsistentIds, responseFutures)) {
                        restarter.initiateRestart();

                        return null;
                    } else {
                        throw new ClusterResetException("Did not get successful responses from new CMG majority, failing cluster reset.");
                    }
                }, restartExecutor);
    }

    private Map<String, CompletableFuture<NetworkMessage>> sendResetClusterMessageTo(Collection<ClusterNode> nodesInTopology,
            ResetClusterMessage message) {
        Map<String, CompletableFuture<NetworkMessage>> responseFutures = new HashMap<>();
        for (ClusterNode node : nodesInTopology) {
            responseFutures.put(node.name(), messagingService.invoke(node, message, 10_000));
        }
        return responseFutures;
    }

    private void ensureNodeIsInitialized() {
        if (!storage.isNodeInitialized()) {
            throw new ClusterResetException("Node is not initialized and cannot serve as a cluster reset conductor.");
        }
    }

    private static void ensureNoRepetitions(List<String> proposedCmgConsistentIds) {
        if (new HashSet<>(proposedCmgConsistentIds).size() != proposedCmgConsistentIds.size()) {
            throw new ClusterResetException("New CMG node consistentIds have repetitions: " + proposedCmgConsistentIds + ".");
        }
    }

    private void ensureContainsThisNodeName(List<String> proposedCmgConsistentIds) {
        if (!proposedCmgConsistentIds.contains(thisNodeName)) {
            throw new ClusterResetException("Current node is not contained in the new CMG, so it cannot conduct a cluster reset.");
        }
    }

    private static void ensureAllProposedCmgNodesAreInTopology(
            List<String> proposedCmgConsistentIds,
            Collection<ClusterNode> nodesInTopology
    ) {
        Set<String> consistentIdsOfNodesInTopology = nodesInTopology.stream().map(ClusterNode::name).collect(toSet());

        Set<String> notInTopology = new HashSet<>(proposedCmgConsistentIds);
        notInTopology.removeAll(consistentIdsOfNodesInTopology);

        if (!notInTopology.isEmpty()) {
            throw new ClusterResetException("Some of proposed CMG nodes are not online: " + notInTopology + ".");
        }
    }

    private ClusterState ensureClusterStateIsPresent() {
        ClusterState clusterState = storage.readClusterState();
        if (clusterState == null) {
            throw new ClusterResetException("Node does not have cluster state.");
        }
        return clusterState;
    }

    private ResetClusterMessage buildResetClusterMessage(List<String> proposedCmgConsistentIds, ClusterState clusterState) {
        List<UUID> formerClusterIds = new ArrayList<>(requireNonNullElse(clusterState.formerClusterIds(), new ArrayList<>()));
        formerClusterIds.add(clusterState.clusterTag().clusterId());

        return messagesFactory.resetClusterMessage()
                .conductor(thisNodeName)
                .cmgNodes(new HashSet<>(proposedCmgConsistentIds))
                .metaStorageNodes(clusterState.metaStorageNodes())
                .clusterName(clusterState.clusterTag().clusterName())
                .clusterId(randomUUID())
                .formerClusterIds(formerClusterIds)
                .build();
    }

    private static boolean isMajorityOfCmgAreSuccesses(
            List<String> proposedCmgConsistentIds,
            Map<String, CompletableFuture<NetworkMessage>> responseFutures
    ) {
        Set<String> newCmgNodesSet = new HashSet<>(proposedCmgConsistentIds);
        List<CompletableFuture<NetworkMessage>> futuresFromNewCmg = responseFutures.entrySet().stream()
                .filter(entry -> newCmgNodesSet.contains(entry.getKey()))
                .map(Entry::getValue)
                .collect(toList());

        assert futuresFromNewCmg.size() == proposedCmgConsistentIds.size()
                : futuresFromNewCmg.size() + " futures, but " + proposedCmgConsistentIds.size() + " nodes";

        long successes = futuresFromNewCmg.stream()
                .filter(CompletableFutures::isCompletedSuccessfully)
                .count();

        return successes >= (futuresFromNewCmg.size() + 1) / 2;
    }

    /**
     * An executor that spawns a thread per task. This is very inefficient for scenarios with many tasks,
     * but we use it here for very rare tasks; it has an advantage that it doesn't need to be shut down
     * (node restart is performed in it, so it might cause troubles to use normal pools created in the Ignite node).
     */
    private static class ThreadPerTaskExecutor implements Executor {
        private final String threadNamePrefix;

        private ThreadPerTaskExecutor(String threadNamePrefix) {
            this.threadNamePrefix = threadNamePrefix;
        }

        @Override
        public void execute(Runnable command) {
            Thread thread = new Thread(command);

            thread.setName(threadNamePrefix + thread.getId());
            thread.setDaemon(true);

            thread.start();
        }
    }
}
