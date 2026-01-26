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

package org.apache.ignite.internal.index;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toZonePartitionIdMessage;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapRootCause;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.partition.replicator.TableTxRwOperationTracker;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.replication.BuildIndexReplicaRequest;
import org.apache.ignite.internal.raft.GroupOverloadedException;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ZonePartitionIdMessage;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.RowMeta;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.distributed.index.MetaIndexStatusChange;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.TrackerClosedException;
import org.jetbrains.annotations.Nullable;

/** Task of building a table index. */
class IndexBuildTask {
    private static final IgniteLogger LOG = Loggers.forClass(IndexBuildTask.class);

    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();

    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private final IndexBuildTaskId taskId;

    private final MetaIndexStatusChange indexCreationInfo;

    private final HybridTimestamp indexBuildingStateActivationTimestamp;

    private final IndexStorage indexStorage;

    private final MvPartitionStorage partitionStorage;

    private final ReplicaService replicaService;

    private final TableTxRwOperationTracker txRwOperationTracker;

    private final PendingComparableValuesTracker<HybridTimestamp, Void> safeTime;

    private final FailureProcessor failureProcessor;

    private final FinalTransactionStateResolver finalTransactionStateResolver;

    private final Executor executor;

    private final IgniteSpinBusyLock busyLock;

    private final int batchSize;

    private final InternalClusterNode node;

    private final List<IndexBuildCompletionListener> buildCompletionListeners;

    private final IndexBuildTaskStatisticsLoggingListener statisticsLoggingListener;

    private final long enlistmentConsistencyToken;

    private final boolean afterDisasterRecovery;

    private final IgniteSpinBusyLock taskBusyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean taskStopGuard = new AtomicBoolean();

    private final CompletableFuture<Void> taskFuture = new CompletableFuture<>();

    private final HybridTimestamp initialOperationTimestamp;

    private final IndexBuilderMetricSource indexBuilderMetricSource;

    IndexBuildTask(
            IndexBuildTaskId taskId,
            MetaIndexStatusChange indexCreationInfo,
            HybridTimestamp indexBuildingStateActivationTimestamp,
            IndexStorage indexStorage,
            MvPartitionStorage partitionStorage,
            ReplicaService replicaService,
            TableTxRwOperationTracker txRwOperationTracker,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTime,
            FailureProcessor failureProcessor,
            FinalTransactionStateResolver finalTransactionStateResolver,
            Executor executor,
            IgniteSpinBusyLock busyLock,
            int batchSize,
            InternalClusterNode node,
            List<IndexBuildCompletionListener> buildCompletionListeners,
            long enlistmentConsistencyToken,
            boolean afterDisasterRecovery,
            HybridTimestamp initialOperationTimestamp,
            IndexBuilderMetricSource indexBuilderMetricSource
    ) {
        this.taskId = taskId;
        this.indexCreationInfo = indexCreationInfo;
        this.indexBuildingStateActivationTimestamp = indexBuildingStateActivationTimestamp;
        this.indexStorage = indexStorage;
        this.partitionStorage = partitionStorage;
        this.replicaService = replicaService;
        this.txRwOperationTracker = txRwOperationTracker;
        this.safeTime = safeTime;
        this.failureProcessor = failureProcessor;
        this.finalTransactionStateResolver = finalTransactionStateResolver;
        this.executor = executor;
        this.busyLock = busyLock;
        this.batchSize = batchSize;
        this.node = node;
        // We do not intentionally make a copy of the list, we want to see changes in the passed list.
        this.buildCompletionListeners = buildCompletionListeners;
        this.statisticsLoggingListener = new IndexBuildTaskStatisticsLoggingListener(taskId, afterDisasterRecovery);
        this.enlistmentConsistencyToken = enlistmentConsistencyToken;
        this.afterDisasterRecovery = afterDisasterRecovery;
        this.initialOperationTimestamp = initialOperationTimestamp;
        this.indexBuilderMetricSource = indexBuilderMetricSource;
    }

    /** Starts building the index. */
    void start() {
        if (!enterBusy()) {
            taskFuture.complete(null);

            return;
        }

        String indexInfo = createCommonIndexInfo();
        if (afterDisasterRecovery) {
            LOG.warn("Start building the index due to disaster recovery of an AVAILABLE index. This shouldn't normally occur [{}]",
                    indexInfo
            );
        } else {
            LOG.info("Start building the index [{}]", indexInfo);
        }

        try {
            // Before starting to build the index, we are waiting for all operations of RW transactions that started before index creation
            // to make sure that, even if some coordinator has gone while we were waiting for its pre-index RW transactions to finish,
            // we still allow operations of those transactions from that coordinator which are still in-flight to finish, so that we
            // index the row versions they could create. Otherwise, we might miss some row versions in the index.
            txRwOperationTracker.awaitCompleteTxRwOperations(indexCreationInfo.catalogVersion())
                    // This wait is necessary to make sure that all writes made before the index has switched to the BUILDING state
                    // are visible to the index build process.
                    .thenCompose(unused -> safeTime.waitFor(indexBuildingStateActivationTimestamp))
                    .thenRun(statisticsLoggingListener::onIndexBuildStarted)
                    .thenApplyAsync(unused -> partitionStorage.highestRowId(), executor)
                    .thenApplyAsync(this::handleNextBatch, executor)
                    .thenCompose(Function.identity())
                    .whenComplete((unused, throwable) -> {
                        if (throwable != null) {
                            if (ignorable(throwable)) {
                                LOG.info("Ignorable index build error [{}, error={}]", indexInfo, unwrapCause(throwable));
                            } else {
                                String message = String.format("Index build error [%s, error=%s]", indexInfo, unwrapCause(throwable));

                                failureProcessor.process(new FailureContext(throwable, message));
                            }

                            taskFuture.completeExceptionally(throwable);
                            statisticsLoggingListener.onIndexBuildFailure(throwable);
                        } else {
                            taskFuture.complete(null);
                            statisticsLoggingListener.onIndexBuildSuccess();
                        }
                    });
        } catch (Throwable t) {
            taskFuture.completeExceptionally(t);

            throw t;
        } finally {
            leaveBusy();
        }
    }

    private static boolean ignorable(Throwable throwable) {
        return hasCause(
                throwable,
                // Following exception can be ignored as IndexBuildController listens for new primary replica appearance, so it will trigger
                // build continuation. We just don't want to fill our logs with garbage.
                PrimaryReplicaMissException.class,
                // Following two can be ignored as they mean that replica is closed (either node is stopping or replica is not needed
                // on this node anymore).
                TrackerClosedException.class,
                StorageClosedException.class,
                // Node is stopping, it's ok.
                NodeStoppingException.class
        );
    }

    /** Stops index building. */
    void stop() {
        if (!taskStopGuard.compareAndSet(false, true)) {
            return;
        }

        taskBusyLock.block();
    }

    /** Returns the index build future. */
    CompletableFuture<Void> getTaskFuture() {
        return taskFuture;
    }

    private CompletableFuture<Void> handleNextBatch(@Nullable RowId highestRowId) {
        if (!enterBusy()) {
            return nullCompletedFuture();
        }

        try {
            indexBuilderMetricSource.onBatchProcessingStarted(taskId);

            return createBatchToIndex(highestRowId)
                    .thenCompose(this::processBatch)
                    .handleAsync((unused, throwable) -> {
                        indexBuilderMetricSource.onBatchProcessingFinished(taskId);

                        if (throwable != null) {
                            Throwable cause = unwrapRootCause(throwable);

                            // Read-write transaction operations have not yet completed, let's try to send the batch again.
                            if (!(cause instanceof ReplicationTimeoutException || cause instanceof GroupOverloadedException)) {
                                return CompletableFuture.<Void>failedFuture(cause);
                            }
                        } else if (indexStorage.getNextRowIdToBuild() == null) {
                            // Index has been built.
                            LOG.info("Index build completed [{}]", createCommonIndexInfo());

                            notifyBuildCompletionListeners(taskId);

                            return CompletableFutures.<Void>nullCompletedFuture();
                        }

                        return handleNextBatch(highestRowId);
                    }, executor)
                    .thenCompose(Function.identity());
        } catch (Throwable t) {
            indexBuilderMetricSource.onBatchProcessingFinished(taskId);

            return failedFuture(t);
        } finally {
            leaveBusy();
        }
    }

    private CompletableFuture<BatchToIndex> createBatchToIndex(@Nullable RowId highestRowId) {
        if (highestRowId == null) {
            return completedFuture(new BatchToIndex(List.of(), Set.of()));
        }

        RowId nextRowIdToBuild = indexStorage.getNextRowIdToBuild();

        List<RowId> rowIds = new ArrayList<>(batchSize);
        Map<UUID, WriteIntentInfo> transactionsToResolve = new HashMap<>();

        List<RowMeta> rows = nextRowIdToBuild == null ? List.of()
                : partitionStorage.rowsStartingWith(nextRowIdToBuild, highestRowId, batchSize);

        for (RowMeta row : rows) {
            rowIds.add(row.rowId());

            if (row.isWriteIntent()) {
                UUID transactionId = row.transactionId();
                assert transactionId != null;

                // We only care about transactions which began after index creation.
                HybridTimestamp txBeginTs = TransactionIds.beginTimestamp(transactionId);
                HybridTimestamp indexCreationTs = hybridTimestamp(indexCreationInfo.activationTimestamp());
                if (txBeginTs.compareTo(indexCreationTs) < 0) {
                    transactionsToResolve.put(
                            row.transactionId(),
                            new WriteIntentInfo(row.commitZoneId(), row.commitPartitionId(), row.newestCommitTimestamp(), row.rowId())
                    );
                }
            }
        }

        Map<UUID, CompletableFuture<TxState>> txStateResolveFutures = transactionsToResolve.entrySet().stream()
                .map(entry -> Map.entry(entry.getKey(), resolveFinalTxStateIfNeeded(entry.getKey(), entry.getValue())))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

        indexBuilderMetricSource.onTransitionToWaitingForTransactions(taskId, txStateResolveFutures.size());

        return CompletableFutures.allOf(txStateResolveFutures.values())
                .thenApply(unused -> {
                    Set<UUID> abortedTransactionIds = txStateResolveFutures.entrySet().stream()
                            .filter(entry -> entry.getValue().join() == TxState.ABORTED)
                            .map(Entry::getKey)
                            .collect(toUnmodifiableSet());

                    return new BatchToIndex(rowIds, abortedTransactionIds);
                });
    }

    private CompletableFuture<TxState> resolveFinalTxStateIfNeeded(UUID transactionId, WriteIntentInfo writeIntentInfo) {
        assert writeIntentInfo.commitZoneId != null;

        ZonePartitionId commitGroupId = new ZonePartitionId(writeIntentInfo.commitZoneId, writeIntentInfo.commitPartitionId);
        ZonePartitionId senderGroupId = new ZonePartitionId(taskId.getZoneId(), taskId.getPartitionId());

        return finalTransactionStateResolver.resolveFinalTxState(
                        transactionId,
                        commitGroupId,
                        senderGroupId,
                        writeIntentInfo.rowId,
                        writeIntentInfo.newestCommitTimestamp
                )
                .thenApply(statisticsLoggingListener::onWriteIntentResolved);
    }

    private CompletableFuture<Void> processBatch(BatchToIndex batch) {
        BuildIndexReplicaRequest request = createBuildIndexReplicaRequest(batch, initialOperationTimestamp);

        indexBuilderMetricSource.onTransitionToWaitingForReplicaResponse(taskId);

        return replicaService.invoke(node, request)
                .whenComplete((unused, throwable) -> {
                    if (throwable == null) {
                        statisticsLoggingListener.onRaftCallSuccess();
                    } else {
                        statisticsLoggingListener.onRaftCallFailure();
                    }
                })
                .thenAccept(unused -> statisticsLoggingListener.onBatchProcessed(batch.rowIds.size()));
    }

    private BuildIndexReplicaRequest createBuildIndexReplicaRequest(BatchToIndex batch, HybridTimestamp initialOperationTimestamp) {
        List<RowId> rowIds = batch.rowIds;
        boolean finish = rowIds.size() < batchSize;

        ZonePartitionIdMessage groupIdMessage =
                toZonePartitionIdMessage(REPLICA_MESSAGES_FACTORY, new ZonePartitionId(taskId.getZoneId(), taskId.getPartitionId()));

        return PARTITION_REPLICATION_MESSAGES_FACTORY.buildIndexReplicaRequest()
                .groupId(groupIdMessage)
                .tableId(taskId.getTableId())
                .indexId(taskId.getIndexId())
                .rowIds(rowIds.stream().map(RowId::uuid).collect(toList()))
                .finish(finish)
                .abortedTransactionIds(batch.abortedTransactionIds)
                .enlistmentConsistencyToken(enlistmentConsistencyToken)
                .timestamp(initialOperationTimestamp)
                .build();
    }

    private boolean enterBusy() {
        return IndexManagementUtils.enterBusy(busyLock, taskBusyLock);
    }

    private void leaveBusy() {
        IndexManagementUtils.leaveBusy(busyLock, taskBusyLock);
    }

    private String createCommonIndexInfo() {
        return IgniteStringFormatter.format(
                "zoneId={}, tableId={}, partitionId={}, indexId={}",
                taskId.getZoneId(), taskId.getTableId(), taskId.getPartitionId(), taskId.getIndexId()
        );
    }

    private void notifyBuildCompletionListeners(IndexBuildTaskId taskId) {
        for (IndexBuildCompletionListener listener : buildCompletionListeners) {
            if (afterDisasterRecovery) {
                listener.onBuildCompletionAfterDisasterRecovery(taskId.getIndexId(), taskId.getTableId(), taskId.getPartitionId());
            } else {
                listener.onBuildCompletion(taskId.getIndexId(), taskId.getTableId(), taskId.getPartitionId());
            }
        }
    }

    private static class BatchToIndex {
        private final List<RowId> rowIds;
        private final Set<UUID> abortedTransactionIds;

        private BatchToIndex(List<RowId> rowIds, Set<UUID> abortedTransactionIds) {
            this.rowIds = rowIds;
            this.abortedTransactionIds = abortedTransactionIds;
        }
    }

    private static class WriteIntentInfo {
        private final @Nullable Integer commitZoneId;
        private final int commitPartitionId;
        private final @Nullable HybridTimestamp newestCommitTimestamp;
        private final RowId rowId;

        private WriteIntentInfo(
                @Nullable Integer commitZoneId,
                int commitPartitionId,
                @Nullable HybridTimestamp newestCommitTimestamp,
                RowId rowId
        ) {
            this.commitZoneId = commitZoneId;
            this.commitPartitionId = commitPartitionId;
            this.newestCommitTimestamp = newestCommitTimestamp;
            this.rowId = rowId;
        }
    }
}
