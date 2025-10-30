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

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
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
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.network.messages.SuccessResponseMessage;
import org.apache.ignite.internal.disaster.system.exception.ClusterResetException;
import org.apache.ignite.internal.disaster.system.exception.MigrateException;
import org.apache.ignite.internal.disaster.system.message.BecomeMetastorageLeaderMessage;
import org.apache.ignite.internal.disaster.system.message.ResetClusterMessage;
import org.apache.ignite.internal.disaster.system.message.ResetClusterMessageBuilder;
import org.apache.ignite.internal.disaster.system.message.StartMetastorageRepairRequest;
import org.apache.ignite.internal.disaster.system.message.StartMetastorageRepairResponse;
import org.apache.ignite.internal.disaster.system.message.SystemDisasterRecoveryMessageGroup;
import org.apache.ignite.internal.disaster.system.message.SystemDisasterRecoveryMessagesFactory;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.impl.MetastorageGroupMaintenance;
import org.apache.ignite.internal.network.ClusterIdSupplier;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.thread.LogUncaughtExceptionHandler;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.vault.VaultManager;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link SystemDisasterRecoveryManager}.
 */
public class SystemDisasterRecoveryManagerImpl implements SystemDisasterRecoveryManager, IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(SystemDisasterRecoveryManagerImpl.class);

    private final String thisNodeName;
    private final TopologyService topologyService;
    private final MessagingService messagingService;
    private final ServerRestarter restarter;
    private final MetastorageGroupMaintenance metastorageGroupMaintenance;
    private final ClusterIdSupplier clusterIdSupplier;

    private final SystemDisasterRecoveryMessagesFactory messagesFactory = new SystemDisasterRecoveryMessagesFactory();
    private static final CmgMessagesFactory cmgMessagesFactory = new CmgMessagesFactory();

    private final SystemDisasterRecoveryStorage storage;

    /** This executor spawns a thread per task and should only be used for very rare tasks. */
    private final Executor restartExecutor;

    private final ClusterManagementGroupManager cmgManager;

    /** Constructor. */
    public SystemDisasterRecoveryManagerImpl(
            String thisNodeName,
            TopologyService topologyService,
            MessagingService messagingService,
            VaultManager vaultManager,
            ServerRestarter restarter,
            MetastorageGroupMaintenance metastorageGroupMaintenance,
            ClusterManagementGroupManager cmgManager,
            ClusterIdSupplier clusterIdSupplier
    ) {
        this.thisNodeName = thisNodeName;
        this.topologyService = topologyService;
        this.messagingService = messagingService;
        this.restarter = restarter;
        this.metastorageGroupMaintenance = metastorageGroupMaintenance;
        this.cmgManager = cmgManager;
        this.clusterIdSupplier = clusterIdSupplier;

        storage = new SystemDisasterRecoveryStorage(vaultManager);
        restartExecutor = new ThreadPerTaskExecutor(thisNodeName + "-restart-");
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        messagingService.addMessageHandler(SystemDisasterRecoveryMessageGroup.class, (message, sender, correlationId) -> {
            if (message instanceof ResetClusterMessage) {
                assert correlationId != null;
                handleResetClusterMessage((ResetClusterMessage) message, sender, correlationId);
            } else if (message instanceof StartMetastorageRepairRequest) {
                assert correlationId != null;
                handleStartMetastorageRepairRequest(sender, correlationId);
            } else if (message instanceof BecomeMetastorageLeaderMessage) {
                assert correlationId != null;
                handleBecomeMetastorageLeaderMessage((BecomeMetastorageLeaderMessage) message, sender, correlationId);
            }
        });

        return nullCompletedFuture();
    }

    private void handleResetClusterMessage(ResetClusterMessage message, InternalClusterNode sender, long correlationId) {
        restartExecutor.execute(() -> {
            storage.saveResetClusterMessage(message);

            // TODO: IGNITE-26396 A node restart might not send a message to the initiator.
            messagingService.respond(sender, successResponseMessage(), correlationId)
                    .thenRunAsync(() -> {
                        if (!thisNodeName.equals(sender.name())) {
                            restarter.initiateRestart();
                        }
                    }, restartExecutor)
                    .whenComplete((res, ex) -> {
                        if (ex != null) {
                            LOG.error("Error when handling a ResetClusterMessage", ex);
                        }
                    });
        });
    }

    private static SuccessResponseMessage successResponseMessage() {
        return cmgMessagesFactory.successResponseMessage().build();
    }

    private void handleStartMetastorageRepairRequest(InternalClusterNode sender, long correlationId) {
        metastorageGroupMaintenance.raftNodeIndex()
                .thenAccept(indexWithTerm -> {
                    storage.saveWitnessedMetastorageRepairClusterId(requiredClusterId());

                    StartMetastorageRepairResponse response = messagesFactory.startMetastorageRepairResponse()
                            .raftIndex(indexWithTerm.index())
                            .raftTerm(indexWithTerm.term())
                            .build();

                    messagingService.respond(sender, response, correlationId);
                })
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        LOG.error("Error when handling a StartMetastorageRepairRequest", ex);
                    }
                });
    }

    private UUID requiredClusterId() {
        return requireNonNull(clusterIdSupplier.clusterId(), "No clusterId yet");
    }

    private void handleBecomeMetastorageLeaderMessage(
            BecomeMetastorageLeaderMessage message,
            InternalClusterNode sender,
            long correlationId
    ) {
        nullCompletedFuture()
                .thenRun(() -> {
                    metastorageGroupMaintenance.initiateForcefulVotersChange(message.termBeforeChange(), message.targetVotingSet());
                    messagingService.respond(sender, successResponseMessage(), correlationId);
                })
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        LOG.error("Error when handling a BecomeMetastorageLeaderMessage", ex);
                    }
                });
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
        if (proposedCmgNodeNames == null) {
            return failedFuture(new ClusterResetException("Proposed CMG node names can't be null."));
        }

        return resetClusterInternal(proposedCmgNodeNames, null);
    }

    @Override
    public CompletableFuture<Void> resetClusterRepairingMetastorage(
            @Nullable List<String> proposedCmgNodeNames,
            int metastorageReplicationFactor
    ) {
        return proposedCmgNodeNamesOrCurrentIfNull(proposedCmgNodeNames)
                .thenCompose(cmgNodeNames -> resetClusterInternal(cmgNodeNames, metastorageReplicationFactor));
    }

    private CompletableFuture<Void> resetClusterInternal(
            Collection<String> proposedCmgNodeNames,
            @Nullable Integer metastorageReplicationFactor
    ) {
        try {
            return doResetCluster(proposedCmgNodeNames, metastorageReplicationFactor);
        } catch (ClusterResetException e) {
            return failedFuture(e);
        }
    }

    private CompletableFuture<Void> doResetCluster(
            Collection<String> proposedCmgNodeNames,
            @Nullable Integer metastorageReplicationFactor
    ) {
        Collection<InternalClusterNode> nodesInTopology = topologyService.allMembers();

        ensureNoRepetitions(proposedCmgNodeNames);
        ensureContainsThisNodeName(proposedCmgNodeNames);

        ensureAllProposedCmgNodesAreInTopology(proposedCmgNodeNames, nodesInTopology);

        ensureReplicationFactorIsPositiveIfGiven(metastorageReplicationFactor);
        ensureReplicationFactorFitsTopologyIfGiven(metastorageReplicationFactor, nodesInTopology);

        ensureInitConfigApplied();
        ClusterState clusterState = ensureClusterStateIsPresent();

        if (metastorageReplicationFactor != null) {
            ensureNoMgMajorityIsOnline(clusterState, nodesInTopology);
        }

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

                    boolean repairMg = metastorageReplicationFactor != null;

                    if (enoughResponsesAreSuccesses(repairMg, proposedCmgNodeNames, responseFutures)) {
                        restarter.initiateRestart();

                        return null;
                    } else {
                        throw new ClusterResetException(errorMessageForNotEnoughSuccesses(repairMg, responseFutures));
                    }
                }, restartExecutor);
    }

    private CompletableFuture<Collection<String>> proposedCmgNodeNamesOrCurrentIfNull(@Nullable List<String> proposedCmgNodeNames) {
        return proposedCmgNodeNames != null
                ? completedFuture(proposedCmgNodeNames)
                : cmgManager.clusterState().thenApply(ClusterState::cmgNodes);
    }

    private static void ensureReplicationFactorIsPositiveIfGiven(@Nullable Integer metastorageReplicationFactor) {
        if (metastorageReplicationFactor != null && metastorageReplicationFactor <= 0) {
            throw new ClusterResetException("Metastorage replication factor must be positive.");
        }
    }

    private static void ensureNoRepetitions(Collection<String> proposedCmgNodeNames) {
        if (new HashSet<>(proposedCmgNodeNames).size() != proposedCmgNodeNames.size()) {
            throw new ClusterResetException("New CMG node names have repetitions: " + proposedCmgNodeNames + ".");
        }
    }

    private void ensureContainsThisNodeName(Collection<String> proposedCmgNodeNames) {
        if (!proposedCmgNodeNames.contains(thisNodeName)) {
            throw new ClusterResetException("Current node is not contained in the new CMG, so it cannot conduct a cluster reset.");
        }
    }

    private void ensureInitConfigApplied() {
        if (!storage.isInitConfigApplied()) {
            throw new ClusterResetException("Initial configuration is not applied, so the node cannot serve as a cluster reset conductor.");
        }
    }

    private static void ensureAllProposedCmgNodesAreInTopology(
            Collection<String> proposedCmgNodeNames,
            Collection<InternalClusterNode> nodesInTopology
    ) {
        Set<String> namesOfNodesInTopology = nodesInTopology.stream().map(InternalClusterNode::name).collect(toSet());

        Set<String> notInTopology = new HashSet<>(proposedCmgNodeNames);
        notInTopology.removeAll(namesOfNodesInTopology);

        if (!notInTopology.isEmpty()) {
            throw new ClusterResetException("Some of proposed CMG nodes are not online: " + notInTopology + ".");
        }
    }

    private static void ensureReplicationFactorFitsTopologyIfGiven(
            @Nullable Integer metastorageReplicationFactor,
            Collection<InternalClusterNode> nodesInTopology
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

    private void ensureNoMgMajorityIsOnline(ClusterState clusterState, Collection<InternalClusterNode> nodesInTopology) {
        Set<String> namesOfNodesInTopology = nodesInTopology.stream()
                .map(InternalClusterNode::name)
                .collect(toSet());

        Set<String> nodes = new HashSet<>(clusterState.metaStorageNodes());
        nodes.retainAll(namesOfNodesInTopology);

        int majority = majoritySizeFor(clusterState.metaStorageNodes().size());
        if (nodes.size() >= majority) {
            throw new ClusterResetException(String.format(
                    "Majority repair is rejected because majority of Metastorage nodes are online [metastorageNodes=%s, onlineNodes=%s].",
                    clusterState.metaStorageNodes(),
                    namesOfNodesInTopology
            ));
        }
    }

    private ResetClusterMessage buildResetClusterMessageForReset(
            Collection<String> proposedCmgNodeNames,
            ClusterState clusterState,
            @Nullable Integer metastorageReplicationFactor,
            Collection<InternalClusterNode> nodesInTopology
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
            builder.participatingNodes(nodesInTopology.stream().map(InternalClusterNode::name).collect(toSet()));
        }

        return builder.build();
    }

    private Map<String, CompletableFuture<NetworkMessage>> sendResetClusterMessageTo(
            Collection<InternalClusterNode> nodesInTopology,
            ResetClusterMessage message
    ) {
        Map<String, CompletableFuture<NetworkMessage>> responseFutures = new HashMap<>();
        for (InternalClusterNode node : nodesInTopology) {
            responseFutures.put(node.name(), messagingService.invoke(node, message, 10_000));
        }
        return responseFutures;
    }

    private static void rethrowIfError(Throwable ex) {
        if (ex instanceof Error) {
            throw (Error) ex;
        }
    }

    private static boolean enoughResponsesAreSuccesses(
            boolean repairMg,
            Collection<String> proposedCmgNodeNames,
            Map<String, CompletableFuture<NetworkMessage>> responseFutures
    ) {
        if (repairMg) {
            return responseFutures.values().stream().allMatch(CompletableFutures::isCompletedSuccessfully);
        } else {
            return isMajorityOfCmgAreSuccesses(proposedCmgNodeNames, responseFutures);
        }
    }

    private static boolean isMajorityOfCmgAreSuccesses(
            Collection<String> proposedCmgNodeNames,
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

        return successes >= majoritySizeFor(futuresFromNewCmg.size());
    }

    private static int majoritySizeFor(int votingMembersCount) {
        return votingMembersCount / 2 + 1;
    }

    private static String errorMessageForNotEnoughSuccesses(
            boolean repairMg,
            Map<String, CompletableFuture<NetworkMessage>> responseFutures
    ) {
        if (repairMg) {
            return String.format(
                    "Did not get successful response from at least one node, failing cluster reset [failedNode=%s].",
                    findAnyFailedNodeName(responseFutures)
            );
        } else {
            return "Did not get successful responses from new CMG majority, failing cluster reset.";
        }
    }

    private static String findAnyFailedNodeName(Map<String, CompletableFuture<NetworkMessage>> responseFutures) {
        return responseFutures.entrySet().stream()
                .filter(entry -> entry.getValue().isCompletedExceptionally())
                .findAny()
                .orElseThrow(() -> new AssertionError("At least one failed future must be present"))
                .getKey();
    }

    @Override
    public CompletableFuture<Void> migrate(ClusterState targetClusterState) {
        try {
            return doMigrate(targetClusterState);
        } catch (MigrateException e) {
            return failedFuture(e);
        }
    }

    private CompletableFuture<Void> doMigrate(ClusterState targetClusterState) {
        if (targetClusterState.formerClusterIds() == null) {
            throw new MigrateException("Migration can only happen using cluster state from a node that saw a cluster reset");
        }

        ClusterState clusterState = ensureClusterStateIsPresent();
        if (isDescendantOrSame(clusterState, targetClusterState)) {
            throw new MigrateException("Migration can only happen from old cluster to new one, not the other way around");
        }

        Collection<InternalClusterNode> nodesInTopology = topologyService.allMembers();

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

    private static boolean isDescendantOrSame(ClusterState potencialDescendant, ClusterState potentialAncestor) {
        List<UUID> descendantLineage = extractClusterIdsLineage(potencialDescendant);
        List<UUID> ancestorLineage = extractClusterIdsLineage(potentialAncestor);

        return descendantLineage.size() >= ancestorLineage.size()
                && descendantLineage.subList(0, ancestorLineage.size()).equals(ancestorLineage);
    }

    private static List<UUID> extractClusterIdsLineage(ClusterState state) {
        List<UUID> lineage = new ArrayList<>();

        List<UUID> formerClusterIds = state.formerClusterIds();
        if (formerClusterIds != null) {
            lineage.addAll(formerClusterIds);
        }

        lineage.add(state.clusterTag().clusterId());

        return lineage;
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
            thread.setUncaughtExceptionHandler(new LogUncaughtExceptionHandler(LOG));

            thread.start();
        }
    }
}
