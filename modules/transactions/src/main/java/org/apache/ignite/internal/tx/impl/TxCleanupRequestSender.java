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
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.impl.TxManagerImpl.TransactionFailureHandler;
import org.apache.ignite.internal.util.CompletableFutures;
import org.jetbrains.annotations.Nullable;

/**
 * Sends TX Cleanup request.
 */
public class TxCleanupRequestSender {
    /** Placement driver helper. */
    private final PlacementDriverHelper placementDriverHelper;

    /** Cleanup processor. */
    private final WriteIntentSwitchProcessor writeIntentSwitchProcessor;

    private final TxMessageSender txMessageSender;

    /**
     * The constructor.
     *
     * @param txMessageSender Message sender.
     * @param placementDriverHelper Placement driver helper.
     * @param writeIntentSwitchProcessor A cleanup processor.
     */
    public TxCleanupRequestSender(
            TxMessageSender txMessageSender,
            PlacementDriverHelper placementDriverHelper,
            WriteIntentSwitchProcessor writeIntentSwitchProcessor
    ) {
        this.txMessageSender = txMessageSender;
        this.placementDriverHelper = placementDriverHelper;
        this.writeIntentSwitchProcessor = writeIntentSwitchProcessor;
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
            Map<TablePartitionId, String> enlistedPartitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        Map<String, Set<TablePartitionId>> partitions = new HashMap<>();
        enlistedPartitions.forEach((partitionId, nodeId) ->
                partitions.computeIfAbsent(nodeId, node -> new HashSet<>()).add(partitionId));

        return cleanupPartitions(partitions, commit, commitTimestamp, txId);
    }

    private CompletableFuture<Void> cleanup(
            Collection<TablePartitionId> partitionIds,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        return placementDriverHelper.findPrimaryReplicas(partitionIds)
                .thenCompose(partitionData -> {
                    switchWriteIntentsOnPartitions(commit, commitTimestamp, txId, partitionData.partitionsWithoutPrimary);

                    return cleanupPartitions(partitionData.partitionsByNode, commit, commitTimestamp, txId);
                });
    }

    private void switchWriteIntentsOnPartitions(
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId,
            Set<TablePartitionId> noPrimaryFound
    ) {
        for (TablePartitionId partition : noPrimaryFound) {
            // Okay, no primary found for that partition.
            // Means the old one is no longer primary thus the locks were released.
            // All we need to do is to wait for the new primary to appear and cleanup write intents.
            writeIntentSwitchProcessor.switchWriteIntentsWithRetry(commit, commitTimestamp, txId, partition);
        }
    }

    private CompletableFuture<Void> cleanupPartitions(
            Map<String, Set<TablePartitionId>> partitionsByNode,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        List<CompletableFuture<Void>> cleanupFutures = new ArrayList<>();

        for (Entry<String, Set<TablePartitionId>> entry : partitionsByNode.entrySet()) {
            String node = entry.getKey();
            Set<TablePartitionId> nodePartitions = entry.getValue();

            cleanupFutures.add(sendCleanupMessageWithRetries(commit, commitTimestamp, txId, node, nodePartitions));
        }

        return allOf(cleanupFutures.toArray(new CompletableFuture<?>[0]));
    }

    private CompletableFuture<Void> sendCleanupMessageWithRetries(
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId,
            String node,
            @Nullable Collection<TablePartitionId> partitions
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

                            return cleanup(partitions, commit, commitTimestamp, txId);
                        }

                        return CompletableFuture.<Void>failedFuture(throwable);
                    }

                    return CompletableFutures.<Void>nullCompletedFuture();
                })
                .thenCompose(v -> v);
    }
}
