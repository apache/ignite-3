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
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestampToLong;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.impl.TxManagerImpl.TransactionFailureHandler;
import org.apache.ignite.internal.tx.message.LockReleaseMessage;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.network.ClusterService;
import org.jetbrains.annotations.Nullable;

/**
 * Sends TX Unlock request.
 */
public class TxUnlockRequestSender {

    /** Network timeout. */
    private static final long RPC_TIMEOUT = 3000;

    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    /** Cluster service. */
    private final ClusterService clusterService;

    /** Placement driver helper. */
    private final PlacementDriverHelper placementDriverHelper;

    /** Hybrid clock. */
    private final HybridClock hybridClock;

    /** Cleanup processor. */
    private final TxCleanupProcessor txCleanupProcessor;

    /**
     * The constructor.
     *
     * @param clusterService Cluster service.
     * @param placementDriverHelper Placement driver helper.
     * @param clock A hybrid logical clock.
     * @param txCleanupProcessor A cleanup processor.
     */
    public TxUnlockRequestSender(
            ClusterService clusterService,
            PlacementDriverHelper placementDriverHelper,
            HybridClock clock,
            TxCleanupProcessor txCleanupProcessor) {
        this.clusterService = clusterService;
        this.placementDriverHelper = placementDriverHelper;
        this.hybridClock = clock;
        this.txCleanupProcessor = txCleanupProcessor;
    }

    /**
     * Sends unlock request to the primary nodes of each one of {@code partitions}.
     *
     * @param partitions Enlisted partition groups.
     * @param commit {@code true} if a commit requested.
     * @param commitTimestamp Commit timestamp ({@code null} if it's an abort).
     * @param txId Transaction id.
     * @return Completable future of Void.
     */
    public CompletableFuture<Void> unlock(
            Collection<TablePartitionId> partitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        return placementDriverHelper.findPrimaryReplicas(partitions, hybridClock.now())
                .thenCompose(partitionData -> {
                    durableCleanupPartitionsWithNoPrimary(commit, commitTimestamp, txId, partitionData.partitionsWithoutPrimary);

                    return unlockPartitions(partitionData.partitionsByNode, commit, commitTimestamp, txId);
                });
    }

    private void durableCleanupPartitionsWithNoPrimary(
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId,
            Set<TablePartitionId> noPrimaryFound
    ) {
        if (!noPrimaryFound.isEmpty()) {
            for (TablePartitionId partition : noPrimaryFound) {
                // Okay, no primary found for that partition.
                // Switch to the durable cleanup that will wait for the primary to appear.
                // Also no need to wait on this future - if the primary has expired, the locks are already released.
                txCleanupProcessor.cleanupWithRetry(commit, commitTimestamp, txId, partition);
            }
        }
    }

    private CompletableFuture<Void> unlockPartitions(
            Map<String, Set<TablePartitionId>> partitionsByNode,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        List<CompletableFuture<Void>> unlockFutures = new ArrayList<>();

        for (Entry<String, Set<TablePartitionId>> entry : partitionsByNode.entrySet()) {
            String node = entry.getKey();
            Set<TablePartitionId> nodePartitions = entry.getValue();

            unlockFutures.add(sendUnlockMessageWithRetries(commit, commitTimestamp, txId, node, nodePartitions));
        }

        return allOf(unlockFutures.toArray(new CompletableFuture<?>[0]));
    }

    private CompletableFuture<Void> sendUnlockMessageWithRetries(
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId,
            String node,
            Collection<TablePartitionId> partitions) {
        Collection<ReplicationGroupId> enlistedPartitions = (Collection<ReplicationGroupId>) (Collection<?>) partitions;

        LockReleaseMessage build = FACTORY.lockReleaseMessage()
                .txId(txId)
                .commit(commit)
                .commitTimestampLong(hybridTimestampToLong(commitTimestamp))
                .timestampLong(hybridClock.nowLong())
                .groups(enlistedPartitions)
                .build();

        return clusterService.messagingService().invoke(node, build, RPC_TIMEOUT)
                .handle((networkMessage, throwable) -> {
                    if (throwable != null) {
                        if (TransactionFailureHandler.isRecoverable(throwable)) {
                            return sendUnlockMessageWithRetries(commit, commitTimestamp, txId, node, partitions);
                        }

                        return CompletableFuture.<Void>failedFuture(throwable);
                    }

                    return CompletableFutures.<Void>nullCompletedFuture();
                })
                .thenCompose(v -> v);
    }
}
