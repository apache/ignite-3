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
import java.util.function.Supplier;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.network.messages.SuccessResponseMessage;
import org.apache.ignite.internal.disaster.system.message.MetastorageIndexTermRequestMessage;
import org.apache.ignite.internal.disaster.system.message.MetastorageIndexTermResponseMessage;
import org.apache.ignite.internal.disaster.system.message.ResetClusterMessage;
import org.apache.ignite.internal.disaster.system.message.ResetClusterMessageBuilder;
import org.apache.ignite.internal.disaster.system.message.SystemDisasterRecoveryMessageGroup;
import org.apache.ignite.internal.disaster.system.message.SystemDisasterRecoveryMessagesFactory;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.raft.IndexWithTerm;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link SystemDisasterRecoveryManager}.
 */
public class SystemDisasterRecoveryManagerImpl implements SystemDisasterRecoveryManager, IgniteComponent {
    private final String thisNodeName;
    private final TopologyService topologyService;
    private final MessagingService messagingService;
    private final ServerRestarter restarter;
    private final Supplier<CompletableFuture<IndexWithTerm>> metastorageIndexWithTerm;

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
            ServerRestarter restarter,
            Supplier<CompletableFuture<IndexWithTerm>> metastorageIndexWithTerm
    ) {
        this.thisNodeName = thisNodeName;
        this.topologyService = topologyService;
        this.messagingService = messagingService;
        this.restarter = restarter;
        this.metastorageIndexWithTerm = metastorageIndexWithTerm;

        storage = new SystemDisasterRecoveryStorage(vaultManager);
        restartExecutor = new ThreadPerTaskExecutor(thisNodeName + "-restart-");
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        messagingService.addMessageHandler(SystemDisasterRecoveryMessageGroup.class, (message, sender, correlationId) -> {
            if (message instanceof ResetClusterMessage) {
                assert correlationId != null;
                handleResetClusterMessage((ResetClusterMessage) message, sender, correlationId);
            } else if (message instanceof MetastorageIndexTermRequestMessage) {
                assert correlationId != null;
                handleMetastorageIndexTermRequest(sender, correlationId);
            }
        });

        return nullCompletedFuture();
    }

    private void handleResetClusterMessage(ResetClusterMessage message, ClusterNode sender, long correlationId) {
        restartExecutor.execute(() -> {
            storage.saveResetClusterMessage(message);

            messagingService.respond(sender, successResponseMessage(), correlationId)
                    .thenRunAsync(() -> {
                        if (!thisNodeName.equals(sender.name())) {
                            restarter.initiateRestart();
                        }
                    }, restartExecutor);
        });
    }

    private void handleMetastorageIndexTermRequest(ClusterNode sender, long correlationId) {
        metastorageIndexWithTerm.get()
                .thenAccept(indexWithTerm -> {
                    MetastorageIndexTermResponseMessage response = messagesFactory.metastorageIndexTermResponseMessage()
                            .raftIndex(indexWithTerm.index())
                            .raftTerm(indexWithTerm.term())
                            .build();

                    messagingService.respond(sender, response, correlationId);
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
    public void markInitConfigApplied() {
        storage.markInitConfigApplied();
    }

    @Override
    public CompletableFuture<Void> resetCluster(List<String> proposedCmgNodeNames) {
        return resetClusterInternal(proposedCmgNodeNames, null);
    }

    @Override
    public CompletableFuture<Void> resetClusterRepairingMetastorage(
            List<String> proposedCmgNodeNames,
            int metastorageReplicationFactor
    ) {
        return resetClusterInternal(proposedCmgNodeNames, metastorageReplicationFactor);
    }

    private CompletableFuture<Void> resetClusterInternal(
            List<String> proposedCmgNodeNames,
            @Nullable Integer metastorageReplicationFactor
    ) {
        try {
            return doResetCluster(proposedCmgNodeNames, metastorageReplicationFactor);
        } catch (ClusterResetException e) {
            return failedFuture(e);
        }
    }

    private CompletableFuture<Void> doResetCluster(List<String> proposedCmgNodeNames, @Nullable Integer metastorageReplicationFactor) {
        ensureReplicationFactorIsPositiveIfGiven(metastorageReplicationFactor);

        ensureNoRepetitions(proposedCmgNodeNames);
        ensureContainsThisNodeName(proposedCmgNodeNames);

        Collection<ClusterNode> nodesInTopology = topologyService.allMembers();
        ensureAllProposedCmgNodesAreInTopology(proposedCmgNodeNames, nodesInTopology);
        ensureReplicationFactorFitsTopologyIfGiven(metastorageReplicationFactor, nodesInTopology);

        ensureInitConfigApplied();
        ClusterState clusterState = ensureClusterStateIsPresent();

        ResetClusterMessage message = buildResetClusterMessageForReset(
                proposedCmgNodeNames,
                clusterState,
                metastorageReplicationFactor,
                nodesInTopology
        );

        Map<String, CompletableFuture<NetworkMessage>> responseFutures = sendResetClusterMessageTo(nodesInTopology, message);

        return allOf(responseFutures.values())
                .handleAsync((res, ex) -> {
                    // We ignore upstream exceptions on purpose.
                    rethrowIfError(ex);

                    if (isMajorityOfCmgAreSuccesses(proposedCmgNodeNames, responseFutures)) {
                        restarter.initiateRestart();

                        return null;
                    } else {
                        throw new ClusterResetException("Did not get successful responses from new CMG majority, failing cluster reset.");
                    }
                }, restartExecutor);
    }

    private static void ensureReplicationFactorIsPositiveIfGiven(@Nullable Integer metastorageReplicationFactor) {
        if (metastorageReplicationFactor != null && metastorageReplicationFactor <= 0) {
            throw new ClusterResetException("Metastorage replication factor must be positive.");
        }
    }

    private static void ensureNoRepetitions(List<String> proposedCmgNodeNames) {
        if (new HashSet<>(proposedCmgNodeNames).size() != proposedCmgNodeNames.size()) {
            throw new ClusterResetException("New CMG node names have repetitions: " + proposedCmgNodeNames + ".");
        }
    }

    private void ensureContainsThisNodeName(List<String> proposedCmgNodeNames) {
        if (!proposedCmgNodeNames.contains(thisNodeName)) {
            throw new ClusterResetException("Current node is not contained in the new CMG, so it cannot conduct a cluster reset.");
        }
    }

    private void ensureInitConfigApplied() {
        if (!storage.isInitConfigApplied()) {
            throw new ClusterResetException("Initial configuration is not applied and cannot serve as a cluster reset conductor.");
        }
    }

    private static void ensureAllProposedCmgNodesAreInTopology(
            List<String> proposedCmgNodeNames,
            Collection<ClusterNode> nodesInTopology
    ) {
        Set<String> namesOfNodesInTopology = nodesInTopology.stream().map(ClusterNode::name).collect(toSet());

        Set<String> notInTopology = new HashSet<>(proposedCmgNodeNames);
        notInTopology.removeAll(namesOfNodesInTopology);

        if (!notInTopology.isEmpty()) {
            throw new ClusterResetException("Some of proposed CMG nodes are not online: " + notInTopology + ".");
        }
    }

    private static void ensureReplicationFactorFitsTopologyIfGiven(
            @Nullable Integer metastorageReplicationFactor,
            Collection<ClusterNode> nodesInTopology
    ) {
        if (metastorageReplicationFactor != null && metastorageReplicationFactor > nodesInTopology.size()) {
            throw new ClusterResetException(
                    "Metastorage replication factor cannot exceed size of current physical topology (" + nodesInTopology.size() + ")."
            );
        }
    }

    private ClusterState ensureClusterStateIsPresent() {
        ClusterState clusterState = storage.readClusterState();
        if (clusterState == null) {
            throw new ClusterResetException("Node does not have cluster state.");
        }
        return clusterState;
    }

    private ResetClusterMessage buildResetClusterMessageForReset(
            Collection<String> proposedCmgNodeNames,
            ClusterState clusterState,
            @Nullable Integer metastorageReplicationFactor,
            Collection<ClusterNode> nodesInTopology
    ) {
        List<UUID> formerClusterIds = new ArrayList<>(requireNonNullElse(clusterState.formerClusterIds(), new ArrayList<>()));
        formerClusterIds.add(clusterState.clusterTag().clusterId());

        ResetClusterMessageBuilder builder = messagesFactory.resetClusterMessage()
                .newCmgNodes(new HashSet<>(proposedCmgNodeNames))
                .currentMetaStorageNodes(clusterState.metaStorageNodes())
                .clusterName(clusterState.clusterTag().clusterName())
                .clusterId(randomUUID())
                .formerClusterIds(formerClusterIds)
                .initialClusterConfiguration(clusterState.initialClusterConfiguration());

        if (metastorageReplicationFactor != null) {
            builder.metastorageReplicationFactor(metastorageReplicationFactor);
            builder.conductor(thisNodeName);
            builder.participatingNodes(nodesInTopology.stream().map(ClusterNode::name).collect(toSet()));
        }

        return builder.build();
    }

    private Map<String, CompletableFuture<NetworkMessage>> sendResetClusterMessageTo(
            Collection<ClusterNode> nodesInTopology,
            ResetClusterMessage message
    ) {
        Map<String, CompletableFuture<NetworkMessage>> responseFutures = new HashMap<>();
        for (ClusterNode node : nodesInTopology) {
            responseFutures.put(node.name(), messagingService.invoke(node, message, 10_000));
        }
        return responseFutures;
    }

    private static void rethrowIfError(Throwable ex) {
        if (ex instanceof Error) {
            throw (Error) ex;
        }
    }

    private static boolean isMajorityOfCmgAreSuccesses(
            List<String> proposedCmgNodeNames,
            Map<String, CompletableFuture<NetworkMessage>> responseFutures
    ) {
        Set<String> newCmgNodesSet = new HashSet<>(proposedCmgNodeNames);
        List<CompletableFuture<NetworkMessage>> futuresFromNewCmg = responseFutures.entrySet().stream()
                .filter(entry -> newCmgNodesSet.contains(entry.getKey()))
                .map(Entry::getValue)
                .collect(toList());

        assert futuresFromNewCmg.size() == proposedCmgNodeNames.size()
                : futuresFromNewCmg.size() + " futures, but " + proposedCmgNodeNames.size() + " nodes";

        long successes = futuresFromNewCmg.stream()
                .filter(CompletableFutures::isCompletedSuccessfully)
                .count();

        return successes >= (futuresFromNewCmg.size() + 1) / 2;
    }

    @Override
    public CompletableFuture<Void> migrate(ClusterState targetClusterState) {
        if (targetClusterState.formerClusterIds() == null) {
            return failedFuture(new MigrateException("Migration can only happen using cluster state from a node that saw a cluster reset"));
        }

        Collection<ClusterNode> nodesInTopology = topologyService.allMembers();

        ResetClusterMessage message = buildResetClusterMessageForMigrate(targetClusterState);

        Map<String, CompletableFuture<NetworkMessage>> responseFutures = sendResetClusterMessageTo(nodesInTopology, message);

        return allOf(responseFutures.values())
                .handleAsync((res, ex) -> {
                    // We ignore upstream exceptions on purpose.
                    rethrowIfError(ex);

                    restarter.initiateRestart();
                    return null;
                }, restartExecutor);
    }

    private ResetClusterMessage buildResetClusterMessageForMigrate(ClusterState clusterState) {
        List<UUID> formerClusterIds = clusterState.formerClusterIds();
        assert formerClusterIds != null : "formerClusterIds is null, but it must never be here as it's from a node that saw a CMG reset; "
                + "current node is " + thisNodeName;

        return messagesFactory.resetClusterMessage()
                .newCmgNodes(clusterState.cmgNodes())
                .currentMetaStorageNodes(clusterState.metaStorageNodes())
                .clusterName(clusterState.clusterTag().clusterName())
                .clusterId(clusterState.clusterTag().clusterId())
                .formerClusterIds(formerClusterIds)
                .initialClusterConfiguration(clusterState.initialClusterConfiguration())
                .build();
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
