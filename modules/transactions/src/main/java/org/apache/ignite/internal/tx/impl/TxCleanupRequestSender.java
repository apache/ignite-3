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

import static java.lang.Math.min;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.logger.Loggers.toThrottledLogger;
import static org.apache.ignite.internal.tx.TxStateMeta.builder;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;
import static org.apache.ignite.internal.util.IgniteUtils.scheduleRetry;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteThrottledLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.ReplicatorRecoverableExceptions;
import org.apache.ignite.internal.replicator.ZonePartitionId;
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
    private static final int ATTEMPTS_LOG_THRESHOLD = 100;

    private static final int RETRY_INITIAL_TIMEOUT_MS = 20;
    private static final int RETRY_MAX_TIMEOUT_MS = 30_000;

    private final IgniteThrottledLogger throttledLog;

    /** Placement driver helper. */
    private final PlacementDriverHelper placementDriverHelper;

    private final TxMessageSender txMessageSender;

    /** The map of txId to a cleanup context, tracking partitions with replicated write intents. */
    private final ConcurrentMap<UUID, CleanupContext> writeIntentsReplicated = new ConcurrentHashMap<>();

    /** Local transaction state storage. */
    private final VolatileTxStateMetaStorage txStateVolatileStorage;

    /** Executor that executes async cleanup actions. */
    private final ExecutorService cleanupExecutor;

    /** Executor that is used to schedule retries of cleanup messages in case of retryable errors. */
    private final ScheduledExecutorService retryExecutor;

    /**
     * The constructor.
     *
     * @param txMessageSender Message sender.
     * @param placementDriverHelper Placement driver helper.
     * @param txStateVolatileStorage Volatile transaction state storage.
     * @param cleanupExecutor Cleanup executor.
     * @param commonScheduler Common scheduler.
     */
    public TxCleanupRequestSender(
            TxMessageSender txMessageSender,
            PlacementDriverHelper placementDriverHelper,
            VolatileTxStateMetaStorage txStateVolatileStorage,
            ExecutorService cleanupExecutor,
            ScheduledExecutorService commonScheduler
    ) {
        this.txMessageSender = txMessageSender;
        this.placementDriverHelper = placementDriverHelper;
        this.txStateVolatileStorage = txStateVolatileStorage;
        this.cleanupExecutor = cleanupExecutor;
        this.retryExecutor = commonScheduler;
        this.throttledLog = toThrottledLogger(Loggers.forClass(TxCleanupRequestSender.class), commonScheduler);
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

    private void markTxnCleanupReplicated(UUID txId, TxState state, ZonePartitionId commitPartitionId) {
        long cleanupCompletionTimestamp = System.currentTimeMillis();

        TxStateMeta txStateMeta = txStateVolatileStorage.state(txId);
        final CompletableFuture<HybridTimestamp> commitTimestampFuture;
        if (state == TxState.COMMITTED && (txStateMeta == null || txStateMeta.commitTimestamp() == null)) {
            commitTimestampFuture = placementDriverHelper.awaitPrimaryReplicaWithExceptionHandling(commitPartitionId)
                    .thenCompose(replicaMeta -> {
                                String primaryNode = replicaMeta.getLeaseholder();
                                HybridTimestamp startTime = replicaMeta.getStartTime();
                                return txMessageSender.resolveTxStateFromCommitPartition(
                                            primaryNode,
                                            txId,
                                            commitPartitionId,
                                            startTime.longValue(),
                                            null,
                                            null
                                        )
                                        .thenApply(TransactionMeta::commitTimestamp);
                            }
                    );
        } else {
            HybridTimestamp existingCommitTs = txStateMeta == null ? null : txStateMeta.commitTimestamp();
            commitTimestampFuture = CompletableFuture.completedFuture(existingCommitTs);
        }

        commitTimestampFuture.thenAccept(commitTimestamp -> txStateVolatileStorage.updateMeta(txId, oldMeta -> builder(oldMeta, state)
                .commitPartitionId(commitPartitionId)
                .commitTimestamp(commitTimestamp)
                .cleanupCompletionTimestamp(cleanupCompletionTimestamp)
                .build())
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
    public CompletableFuture<Void> cleanup(ZonePartitionId commitPartitionId, String node, UUID txId) {
        return sendCleanupMessageWithRetries(commitPartitionId, false, null, txId, node, null, RETRY_INITIAL_TIMEOUT_MS, 0);
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
            @Nullable ZonePartitionId commitPartitionId,
            Map<ZonePartitionId, PartitionEnlistment> enlistedPartitions,
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

        return cleanupPartitions(commitPartitionId, partitionsByPrimaryName, commit, commitTimestamp, txId, RETRY_INITIAL_TIMEOUT_MS, 0);
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
            @Nullable ZonePartitionId commitPartitionId,
            Collection<EnlistedPartitionGroup> partitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        return cleanup(commitPartitionId, partitions, commit, commitTimestamp, txId, RETRY_INITIAL_TIMEOUT_MS, 0);
    }

    private CompletableFuture<Void> cleanup(
            @Nullable ZonePartitionId commitPartitionId,
            Collection<EnlistedPartitionGroup> partitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId,
            long timeout,
            int attemptsMade
    ) {
        Map<ZonePartitionId, EnlistedPartitionGroup> partitionIds = partitions.stream()
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
                            toPartitionInfos(partitionData.partitionsWithoutPrimary, partitionIds),
                            timeout,
                            attemptsMade
                    );

                    Map<String, List<EnlistedPartitionGroup>> partitionsByPrimaryName = toPartitionInfosByPrimaryName(
                            partitionData.partitionsByNode,
                            partitionIds
                    );
                    return cleanupPartitions(
                            commitPartitionId,
                            partitionsByPrimaryName,
                            commit,
                            commitTimestamp,
                            txId,
                            timeout,
                            attemptsMade
                    );
                });
    }

    private static Map<String, List<EnlistedPartitionGroup>> toPartitionInfosByPrimaryName(
            Map<String, Set<ZonePartitionId>> partitionsByNode,
            Map<ZonePartitionId, EnlistedPartitionGroup> partitionIds
    ) {
        return partitionsByNode.entrySet().stream()
                .collect(toMap(Entry::getKey, entry -> toPartitionInfos(entry.getValue(), partitionIds)));
    }

    private static List<EnlistedPartitionGroup> toPartitionInfos(
            Set<ZonePartitionId> groupIds,
            Map<ZonePartitionId, EnlistedPartitionGroup> partitionIds
    ) {
        return groupIds.stream()
                .map(partitionIds::get)
                .collect(toList());
    }

    private void cleanupPartitionsWithoutPrimary(
            @Nullable ZonePartitionId commitPartitionId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId,
            List<EnlistedPartitionGroup> partitionsWithoutPrimary,
            long timeout,
            int attemptsMade
    ) {
        Map<ZonePartitionId, EnlistedPartitionGroup> partitionIds = partitionsWithoutPrimary.stream()
                .collect(toMap(EnlistedPartitionGroup::groupId, identity()));

        // For the partitions without primary, we need to wait until a new primary is found.
        // Then we can proceed with the common cleanup flow.
        placementDriverHelper.awaitPrimaryReplicas(partitionIds.keySet())
                .thenCompose(partitionIdsByPrimaryName -> {
                    Map<String, List<EnlistedPartitionGroup>> partitionsByPrimaryName = toPartitionInfosByPrimaryName(
                            partitionIdsByPrimaryName,
                            partitionIds
                    );
                    return cleanupPartitions(
                            commitPartitionId,
                            partitionsByPrimaryName,
                            commit,
                            commitTimestamp,
                            txId,
                            timeout,
                            attemptsMade
                    );
                });
    }

    private CompletableFuture<Void> cleanupPartitions(
            @Nullable ZonePartitionId commitPartitionId,
            Map<String, List<EnlistedPartitionGroup>> partitionsByNode,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId,
            long timeout,
            int attemptsMade
    ) {
        List<CompletableFuture<Void>> cleanupFutures = new ArrayList<>();

        for (Entry<String, List<EnlistedPartitionGroup>> entry : partitionsByNode.entrySet()) {
            String node = entry.getKey();
            List<EnlistedPartitionGroup> nodePartitions = entry.getValue();

            cleanupFutures.add(sendCleanupMessageWithRetries(commitPartitionId, commit, commitTimestamp, txId, node,
                    commitPartitionId == null ? null : nodePartitions, timeout, attemptsMade));
        }

        return allOf(cleanupFutures.toArray(new CompletableFuture<?>[0]));
    }

    private CompletableFuture<Void> sendCleanupMessageWithRetries(
            @Nullable ZonePartitionId commitPartitionId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId,
            String node,
            @Nullable Collection<EnlistedPartitionGroup> partitions,
            long timeout,
            int attemptsMade
    ) {
        return txMessageSender.cleanup(node, partitions, txId, commit, commitTimestamp)
                .thenApply(response -> {
                    if (response instanceof TxCleanupMessageErrorResponse) {
                        TxCleanupMessageErrorResponse errorResponse = (TxCleanupMessageErrorResponse) response;

                        sneakyThrow(errorResponse.throwable());
                    }

                    return response;
                })
                .handleAsync((networkMessage, throwable) -> {
                    if (throwable != null) {
                        if (ReplicatorRecoverableExceptions.isRecoverable(throwable)) {
                            if (attemptsMade > ATTEMPTS_LOG_THRESHOLD) {
                                throttledLog.warn(
                                        "Unsuccessful transaction cleanup after {} attempts, keep retrying [txId={}]",
                                        throwable,
                                        ATTEMPTS_LOG_THRESHOLD,
                                        txId
                                );
                            }

                            // In the case of a failure we repeat the process, but start with finding correct primary replicas
                            // for this subset of partitions. If nothing changed in terms of the nodes and primaries
                            // we eventually will call ourselves with the same parameters.
                            // On the other hand (for example if this node has died) we will
                            //  either have a new mapping of primary to its partitions
                            // or will run `switchWriteIntentsOnPartitions` for partitions with no primary.
                            // At the end of the day all write intents will be properly converted.
                            if (partitions == null) {
                                // If we don't have any partition, which is the recovery or "unlock only" case,
                                // just try again with the same node.
                                return sendCleanupMessageWithRetries(
                                        commitPartitionId,
                                        commit,
                                        commitTimestamp,
                                        txId,
                                        node,
                                        partitions,
                                        incrementTimeout(timeout),
                                        attemptsMade + 1
                                );
                            }

                            // Run a cleanup that finds new primaries for the given partitions.
                            // This covers the case when a partition primary died and we still want to switch write intents.
                            return scheduleRetry(
                                    () -> cleanup(
                                        commitPartitionId,
                                        partitions,
                                        commit,
                                        commitTimestamp,
                                        txId,
                                        incrementTimeout(timeout),
                                        attemptsMade + 1
                                    ),
                                    timeout,
                                    TimeUnit.MILLISECONDS,
                                    retryExecutor
                            );
                        }

                        return CompletableFuture.<Void>failedFuture(throwable);
                    }

                    return CompletableFutures.<Void>nullCompletedFuture();
                }, cleanupExecutor)
                .thenCompose(v -> v);
    }

    private static long incrementTimeout(long currentTimeout) {
        return min(currentTimeout * 2, RETRY_MAX_TIMEOUT_MS);
    }

    private static class CleanupContext {
        private final ZonePartitionId commitPartitionId;

        /**
         * The partitions the we have not received write intent replication confirmation for.
         */
        private final Set<ZonePartitionId> partitions;

        /**
         * The state of the transaction.
         */
        private final TxState txState;

        private CleanupContext(ZonePartitionId commitPartitionId, Set<ZonePartitionId> partitions, TxState txState) {
            this.commitPartitionId = commitPartitionId;
            this.partitions = partitions;
            this.txState = txState;
        }
    }

}
