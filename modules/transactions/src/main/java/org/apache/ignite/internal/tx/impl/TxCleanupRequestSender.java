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
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.impl.TxManagerImpl.TransactionFailureHandler;
import org.apache.ignite.internal.tx.message.CleanupReplicatedInfo;
import org.apache.ignite.internal.tx.message.TxCleanupMessageErrorResponse;
import org.apache.ignite.internal.tx.message.TxCleanupMessageResponse;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.util.CompletableFutures;
import org.jetbrains.annotations.Nullable;

/**
 * Sends TX Cleanup request.
 */
public class TxCleanupRequestSender {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TxCleanupRequestSender.class);

    /** Placement driver helper. */
    private final PlacementDriverHelper placementDriverHelper;

    private final TxMessageSender txMessageSender;

    /** The map of txId to a cleanup context, tracking partitions with replicated write intents. */
    private final ConcurrentMap<UUID, CleanupContext> writeIntentsReplicated = new ConcurrentHashMap<>();

    /** Local transaction state storage. */
    private final VolatileTxStateMetaStorage txStateVolatileStorage;

    /**
     * The constructor.
     *
     * @param txMessageSender Message sender.
     * @param placementDriverHelper Placement driver helper.
     * @param txStateVolatileStorage Volatile transaction state storage.
     */
    public TxCleanupRequestSender(
            TxMessageSender txMessageSender,
            PlacementDriverHelper placementDriverHelper,
            VolatileTxStateMetaStorage txStateVolatileStorage
    ) {
        this.txMessageSender = txMessageSender;
        this.placementDriverHelper = placementDriverHelper;
        this.txStateVolatileStorage = txStateVolatileStorage;
    }

    /**
     * Starts the request sender.
     */
    public void start() {
        txMessageSender.messagingService().addMessageHandler(TxMessageGroup.class, (msg, sender, correlationId) -> {
            if (msg instanceof TxCleanupMessageResponse && correlationId == null) {
                CleanupReplicatedInfo result = ((TxCleanupMessageResponse) msg).result();

                if (result != null) {
                    onCleanupReplicated(result);
                }

                if (msg instanceof TxCleanupMessageErrorResponse) {
                    TxCleanupMessageErrorResponse response = (TxCleanupMessageErrorResponse) msg;

                    LOG.warn("Exception happened during transaction cleanup [txId={}].", response.throwable(), response.txId());
                }
            }
        });
    }

    private void onCleanupReplicated(CleanupReplicatedInfo info) {
        CleanupContext ctx = writeIntentsReplicated.computeIfPresent(info.txId(), (uuid, cleanupContext) -> {
            cleanupContext.partitions.removeAll(info.partitions());

            return cleanupContext;
        });

        if (ctx != null && ctx.partitions.isEmpty()) {
            markTxnCleanupReplicated(info.txId(), ctx.txState);

            writeIntentsReplicated.remove(info.txId());
        }
    }

    private void markTxnCleanupReplicated(UUID txId, TxState state) {
        long cleanupCompletionTimestamp = System.currentTimeMillis();

        txStateVolatileStorage.updateMeta(txId, oldMeta ->
                new TxStateMeta(oldMeta == null ? state : oldMeta.txState(),
                        oldMeta == null ? null : oldMeta.txCoordinatorId(),
                        oldMeta == null ? null : oldMeta.commitPartitionId(),
                        oldMeta == null ? null : oldMeta.commitTimestamp(),
                        oldMeta == null ? null : oldMeta.initialVacuumObservationTimestamp(),
                        cleanupCompletionTimestamp)
        );
    }

    /**
     * Sends unlock request to the nodes than initiated recovery.
     *
     * @param node Target node.
     * @param txId Transaction id.
     * @return Completable future of Void.
     */
    public CompletableFuture<Void> cleanup(String node, UUID txId) {
        return sendCleanupMessageWithRetries(false, null, txId, node, null);
    }

    /**
     * Sends cleanup request to the primary nodes of each one of {@code partitions}.
     *
     * @param enlistedPartitions Map of enlisted partition group to the initial primary node.
     * @param commit {@code true} if a commit requested.
     * @param commitTimestamp Commit timestamp ({@code null} if it's an abort).
     * @param txId Transaction id.
     * @return Completable future of Void.
     */
    public CompletableFuture<Void> cleanup(
            Map<ZonePartitionId, String> enlistedPartitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        // Start tracking the partitions we want to learn the replication confirmation from.
        writeIntentsReplicated.put(txId, new CleanupContext(enlistedPartitions.keySet(), commit ? TxState.COMMITTED : TxState.ABORTED));

        Map<String, Set<ZonePartitionId>> partitions = new HashMap<>();
        enlistedPartitions.forEach((partitionId, nodeId) ->
                partitions.computeIfAbsent(nodeId, node -> new HashSet<>()).add(partitionId));

        return cleanupPartitions(partitions, commit, commitTimestamp, txId);
    }

    /**
     * Gets primary nodes for each of the provided {@code partitions} and sends cleanup request to each one.
     *
     * @param partitionIds Collection of enlisted partition groups.
     * @param commit {@code true} if a commit requested.
     * @param commitTimestamp Commit timestamp ({@code null} if it's an abort).
     * @param txId Transaction id.
     * @return Completable future of Void.
     */
    public CompletableFuture<Void> cleanup(
            Collection<ZonePartitionId> partitionIds,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        // Start tracking the partitions we want to learn the replication confirmation from.
        writeIntentsReplicated.put(txId, new CleanupContext(new HashSet<>(partitionIds), commit ? TxState.COMMITTED : TxState.ABORTED));

        return placementDriverHelper.findPrimaryReplicas(partitionIds)
                .thenCompose(partitionData -> {
                    cleanupPartitionsWithoutPrimary(commit, commitTimestamp, txId, partitionData.partitionsWithoutPrimary);

                    return cleanupPartitions(partitionData.partitionsByNode, commit, commitTimestamp, txId);
                });
    }

    private void cleanupPartitionsWithoutPrimary(
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId,
            Set<ZonePartitionId> noPrimaryFound
    ) {
        // For the partitions without primary, we need to wait until a new primary is found.
        // Then we can proceed with the common cleanup flow.
        placementDriverHelper.awaitPrimaryReplicas(noPrimaryFound)
                .thenCompose(partitionsByNode -> cleanupPartitions(partitionsByNode, commit, commitTimestamp, txId));
    }

    private CompletableFuture<Void> cleanupPartitions(
            Map<String, Set<ZonePartitionId>> partitionsByNode,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        List<CompletableFuture<Void>> cleanupFutures = new ArrayList<>();

        for (Entry<String, Set<ZonePartitionId>> entry : partitionsByNode.entrySet()) {
            String node = entry.getKey();
            Set<ZonePartitionId> nodePartitions = entry.getValue();

            cleanupFutures.add(sendCleanupMessageWithRetries(commit, commitTimestamp, txId, node, nodePartitions));
        }

        return allOf(cleanupFutures.toArray(new CompletableFuture<?>[0]));
    }

    private CompletableFuture<Void> sendCleanupMessageWithRetries(
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId,
            String node,
            @Nullable Collection<ZonePartitionId> partitions
    ) {
        Collection<ReplicationGroupId> enlistedPartitions = (Collection<ReplicationGroupId>) (Collection<?>) partitions;

        return txMessageSender.cleanup(node, enlistedPartitions, txId, commit, commitTimestamp)
                .handle((networkMessage, throwable) -> {
                    if (throwable != null) {
                        if (TransactionFailureHandler.isRecoverable(throwable)) {
                            // In the case of a failure we repeat the process, but start with finding correct primary replicas
                            // for this subset of partitions. If nothing changed in terms of the nodes and primaries
                            // we eventually will call ourselves with the same parameters.
                            // On the other hand (for example if this node has died) we will
                            //  either have a new mapping of primary to its partitions
                            // or will run `switchWriteIntentsOnPartitions` for partitions with no primary.
                            // At the end of the day all write intents will be properly converted.
                            if (partitions == null) {
                                // If we don't have any partition, which is the recovery case,
                                // just try again with the same node.
                                return sendCleanupMessageWithRetries(commit, commitTimestamp, txId, node, partitions);
                            }

                            // Run a cleanup that finds new primaries for the given partitions.
                            // This covers the case when a partition primary died and we still want to switch write intents.
                            return cleanup(partitions, commit, commitTimestamp, txId);
                        }

                        return CompletableFuture.<Void>failedFuture(throwable);
                    }

                    return CompletableFutures.<Void>nullCompletedFuture();
                })
                .thenCompose(v -> v);
    }

    private static class CleanupContext {

        /**
         * The partitions the we have not received write intent replication confirmation for.
         */
        private final Set<ZonePartitionId> partitions;

        /**
         * The state of the transaction.
         */
        private final TxState txState;

        private CleanupContext(Set<ZonePartitionId> partitions, TxState txState) {
            this.partitions = partitions;

            this.txState = txState;
        }
    }

}
