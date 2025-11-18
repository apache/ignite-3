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

package org.apache.ignite.internal.tx.impl;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;
import static org.apache.ignite.internal.tx.impl.TxCleanupExceptionUtils.writeIntentSwitchFailureShouldBeLogged;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ReplicatorRecoverableExceptions;
import org.apache.ignite.internal.tx.PartitionEnlistment;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.message.CleanupReplicatedInfo;
import org.apache.ignite.internal.tx.message.CleanupReplicatedInfoMessage;
import org.apache.ignite.internal.tx.message.TxCleanupMessageErrorResponse;
import org.apache.ignite.internal.tx.message.TxCleanupMessageResponse;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.util.CompletableFutures;
import org.jetbrains.annotations.Nullable;

/**
 * Sends TX Cleanup request.
 */
public class TxCleanupRequestSender {
    private static final IgniteLogger LOG = Loggers.forClass(TxCleanupRequestSender.class);

    /** Placement driver helper. */
    private final PlacementDriverHelper placementDriverHelper;

    private final TxMessageSender txMessageSender;

    /** The map of txId to a cleanup context, tracking partitions with replicated write intents. */
    private final ConcurrentMap<UUID, CleanupContext> writeIntentsReplicated = new ConcurrentHashMap<>();

    /** Local transaction state storage. */
    private final VolatileTxStateMetaStorage txStateVolatileStorage;

    /** Local node's consistent id. */
    private final String nodeName;

    private ClockService clockService;

    /**
     * The constructor.
     *
     * @param txMessageSender Message sender.
     * @param placementDriverHelper Placement driver helper.
     * @param txStateVolatileStorage Volatile transaction state storage.
     * @param nodeName Local node's consistent id.
     * @param clockService Clock service.
     */
    public TxCleanupRequestSender(
            TxMessageSender txMessageSender,
            PlacementDriverHelper placementDriverHelper,
            VolatileTxStateMetaStorage txStateVolatileStorage,
            String nodeName,
            ClockService clockService
    ) {
        this.txMessageSender = txMessageSender;
        this.placementDriverHelper = placementDriverHelper;
        this.txStateVolatileStorage = txStateVolatileStorage;
        this.nodeName = nodeName;
        this.clockService = clockService;
    }

    /**
     * Starts the request sender.
     */
    public void start() {
        txMessageSender.messagingService().addMessageHandler(TxMessageGroup.class, (msg, sender, correlationId) -> {
            // correlationId == null means we get the second TxCleanupMessageResponse
            // that gets sent when cleanup is replicated to the majority.
            if (msg instanceof TxCleanupMessageResponse && correlationId == null) {
                // The cleanup response is sent only in the success case, hence no error is expected.
                assert !(msg instanceof TxCleanupMessageErrorResponse) : "Cleanup error response is not expected here.";

                CleanupReplicatedInfoMessage result = ((TxCleanupMessageResponse) msg).result();

                assert result != null : "Result for the cleanup response cannot be null.";

                onCleanupReplicated(result.asCleanupReplicatedInfo());
            }
        });
    }

    private void onCleanupReplicated(CleanupReplicatedInfo info) {
        CleanupContext ctx = writeIntentsReplicated.computeIfPresent(info.txId(), (uuid, cleanupContext) -> {
            cleanupContext.partitions.removeAll(info.partitions());

            return cleanupContext;
        });

        if (ctx != null && ctx.partitions.isEmpty()) {
            markTxnCleanupReplicated(info.txId(), ctx.txState, ctx.commitPartitionId);

            writeIntentsReplicated.remove(info.txId());
        }
    }

    private void markTxnCleanupReplicated(UUID txId, TxState state, ReplicationGroupId commitPartitionId) {
        long cleanupCompletionTimestamp = System.currentTimeMillis();

        final HybridTimestamp commitTimestamp;
        if (state == COMMITTED && txStateVolatileStorage.state(txId) == null) {
            TransactionMeta transactionMeta = txMessageSender.resolveTxStateFromCommitPartition(
                    nodeName,
                    txId,
                    commitPartitionId,
                    clockService.currentLong()).join();
            commitTimestamp = transactionMeta.commitTimestamp();
        } else {
            commitTimestamp = null;
        }

        txStateVolatileStorage.updateMeta(txId, oldMeta ->
                new TxStateMeta(
                        oldMeta == null ? state : oldMeta.txState(),
                        oldMeta == null ? null : oldMeta.txCoordinatorId(),
                        commitPartitionId,
                        oldMeta == null ? commitTimestamp : oldMeta.commitTimestamp(),
                        oldMeta == null ? null : oldMeta.tx(),
                        oldMeta == null ? null : oldMeta.initialVacuumObservationTimestamp(),
                        cleanupCompletionTimestamp,
                        oldMeta == null ? null : oldMeta.isFinishedDueToTimeout()
                )
        );
    }

    /**
     * Sends unlock request to the nodes than initiated recovery.
     *
     * @param commitPartitionId Commit partition id.
     * @param node Target node.
     * @param txId Transaction id.
     * @return Completable future of Void.
     */
    public CompletableFuture<Void> cleanup(ReplicationGroupId commitPartitionId, String node, UUID txId) {
        return sendCleanupMessageWithRetries(commitPartitionId, false, null, txId, node, null);
    }

    /**
     * Sends cleanup request to the primary nodes of each one of {@code partitions}.
     *
     * @param commitPartitionId Commit partition id. {@code Null} for unlock only path.
     * @param enlistedPartitions Map of enlisted partitions.
     * @param commit {@code true} if a commit requested.
     * @param commitTimestamp Commit timestamp ({@code null} if it's an abort).
     * @param txId Transaction id.
     * @return Completable future of Void.
     */
    public CompletableFuture<Void> cleanup(
            @Nullable ReplicationGroupId commitPartitionId,
            Map<ReplicationGroupId, PartitionEnlistment> enlistedPartitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        // Start tracking the partitions we want to learn the replication confirmation from.
        if (commitPartitionId != null) {
            writeIntentsReplicated.put(
                    txId,
                    new CleanupContext(commitPartitionId, enlistedPartitions.keySet(), commit ? TxState.COMMITTED : TxState.ABORTED)
            );
        }

        Map<String, List<EnlistedPartitionGroup>> partitionsByPrimaryName = new HashMap<>();
        enlistedPartitions.forEach((partitionId, partition) -> {
            List<EnlistedPartitionGroup> enlistedPartitionGroups = partitionsByPrimaryName.computeIfAbsent(
                    partition.primaryNodeConsistentId(),
                    node -> new ArrayList<>()
            );
            enlistedPartitionGroups.add(new EnlistedPartitionGroup(partitionId, partition.tableIds()));
        });

        return cleanupPartitions(commitPartitionId, partitionsByPrimaryName, commit, commitTimestamp, txId);
    }

    /**
     * Gets primary nodes for each of the provided {@code partitions} and sends cleanup request to each one.
     *
     * @param commitPartitionId Commit partition id. {@code Null} for unlock only path.
     * @param partitions Collection of enlisted partitions.
     * @param commit {@code true} if a commit requested.
     * @param commitTimestamp Commit timestamp ({@code null} if it's an abort).
     * @param txId Transaction id.
     * @return Completable future of Void.
     */
    public CompletableFuture<Void> cleanup(
            @Nullable ReplicationGroupId commitPartitionId,
            Collection<EnlistedPartitionGroup> partitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        Map<ReplicationGroupId, EnlistedPartitionGroup> partitionIds = partitions.stream()
                .collect(toMap(EnlistedPartitionGroup::groupId, identity()));

        if (commitPartitionId != null) {
            // Start tracking the partitions we want to learn the replication confirmation from.
            writeIntentsReplicated.put(
                    txId,
                    new CleanupContext(commitPartitionId, new HashSet<>(partitionIds.keySet()),
                            commit ? TxState.COMMITTED : TxState.ABORTED)
            );
        }

        return placementDriverHelper.findPrimaryReplicas(partitionIds.keySet())
                .thenCompose(partitionData -> {
                    cleanupPartitionsWithoutPrimary(
                            commitPartitionId,
                            commit,
                            commitTimestamp,
                            txId,
                            toPartitionInfos(partitionData.partitionsWithoutPrimary, partitionIds)
                    );

                    Map<String, List<EnlistedPartitionGroup>> partitionsByPrimaryName = toPartitionInfosByPrimaryName(
                            partitionData.partitionsByNode,
                            partitionIds
                    );
                    return cleanupPartitions(commitPartitionId, partitionsByPrimaryName, commit, commitTimestamp, txId);
                });
    }

    private static Map<String, List<EnlistedPartitionGroup>> toPartitionInfosByPrimaryName(
            Map<String, Set<ReplicationGroupId>> partitionsByNode,
            Map<ReplicationGroupId, EnlistedPartitionGroup> partitionIds
    ) {
        return partitionsByNode.entrySet().stream()
                .collect(toMap(Entry::getKey, entry -> toPartitionInfos(entry.getValue(), partitionIds)));
    }

    private static List<EnlistedPartitionGroup> toPartitionInfos(
            Set<ReplicationGroupId> groupIds,
            Map<ReplicationGroupId, EnlistedPartitionGroup> partitionIds
    ) {
        return groupIds.stream()
                .map(partitionIds::get)
                .collect(toList());
    }

    private void cleanupPartitionsWithoutPrimary(
            @Nullable ReplicationGroupId commitPartitionId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId,
            List<EnlistedPartitionGroup> partitionsWithoutPrimary
    ) {
        Map<ReplicationGroupId, EnlistedPartitionGroup> partitionIds = partitionsWithoutPrimary.stream()
                .collect(toMap(EnlistedPartitionGroup::groupId, identity()));

        // For the partitions without primary, we need to wait until a new primary is found.
        // Then we can proceed with the common cleanup flow.
        placementDriverHelper.awaitPrimaryReplicas(partitionIds.keySet())
                .thenCompose(partitionIdsByPrimaryName -> {
                    Map<String, List<EnlistedPartitionGroup>> partitionsByPrimaryName = toPartitionInfosByPrimaryName(
                            partitionIdsByPrimaryName,
                            partitionIds
                    );
                    return cleanupPartitions(commitPartitionId, partitionsByPrimaryName, commit, commitTimestamp, txId);
                });
    }

    private CompletableFuture<Void> cleanupPartitions(
            @Nullable ReplicationGroupId commitPartitionId,
            Map<String, List<EnlistedPartitionGroup>> partitionsByNode,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        List<CompletableFuture<Void>> cleanupFutures = new ArrayList<>();

        for (Entry<String, List<EnlistedPartitionGroup>> entry : partitionsByNode.entrySet()) {
            String node = entry.getKey();
            List<EnlistedPartitionGroup> nodePartitions = entry.getValue();

            cleanupFutures.add(sendCleanupMessageWithRetries(commitPartitionId, commit, commitTimestamp, txId, node,
                    commitPartitionId == null ? null : nodePartitions));
        }

        return allOf(cleanupFutures.toArray(new CompletableFuture<?>[0]));
    }

    private CompletableFuture<Void> sendCleanupMessageWithRetries(
            @Nullable ReplicationGroupId commitPartitionId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId,
            String node,
            @Nullable Collection<EnlistedPartitionGroup> partitions
    ) {
        return txMessageSender.cleanup(node, partitions, txId, commit, commitTimestamp)
                .handle((networkMessage, throwable) -> {
                    if (throwable != null) {
                        if (ReplicatorRecoverableExceptions.isRecoverable(throwable)) {
                            // In the case of a failure we repeat the process, but start with finding correct primary replicas
                            // for this subset of partitions. If nothing changed in terms of the nodes and primaries
                            // we eventually will call ourselves with the same parameters.
                            // On the other hand (for example if this node has died) we will
                            //  either have a new mapping of primary to its partitions
                            // or will run `switchWriteIntentsOnPartitions` for partitions with no primary.
                            // At the end of the day all write intents will be properly converted.
                            if (partitions == null) {
                                // If we don't have any partition, which is the recovery or unlock only case,
                                // just try again with the same node.
                                return sendCleanupMessageWithRetries(commitPartitionId, commit, commitTimestamp, txId, node, partitions);
                            }

                            // Run a cleanup that finds new primaries for the given partitions.
                            // This covers the case when a partition primary died and we still want to switch write intents.
                            return cleanup(commitPartitionId, partitions, commit, commitTimestamp, txId);
                        }

                        return CompletableFuture.<Void>failedFuture(throwable);
                    }

                    if (networkMessage instanceof TxCleanupMessageErrorResponse) {
                        TxCleanupMessageErrorResponse errorResponse = (TxCleanupMessageErrorResponse) networkMessage;
                        if (writeIntentSwitchFailureShouldBeLogged(errorResponse.throwable())) {
                            LOG.warn(
                                    "First cleanup attempt failed (the transaction outcome is not affected) [txId={}]",
                                    errorResponse.throwable(), txId
                            );
                        }

                        // We don't fail the resulting future as a failing cleanup is not a problem.
                    }

                    return CompletableFutures.<Void>nullCompletedFuture();
                })
                .thenCompose(v -> v);
    }

    private static class CleanupContext {
        private final ReplicationGroupId commitPartitionId;

        /**
         * The partitions the we have not received write intent replication confirmation for.
         */
        private final Set<ReplicationGroupId> partitions;

        /**
         * The state of the transaction.
         */
        private final TxState txState;

        private CleanupContext(ReplicationGroupId commitPartitionId, Set<ReplicationGroupId> partitions, TxState txState) {
            this.commitPartitionId = commitPartitionId;
            this.partitions = partitions;
            this.txState = txState;
        }
    }

}
