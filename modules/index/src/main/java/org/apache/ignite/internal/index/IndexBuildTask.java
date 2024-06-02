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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.ignite.internal.datareplication.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.datareplication.network.replication.BuildIndexReplicaRequest;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;

/** Task of building a table index. */
class IndexBuildTask {
    private static final IgniteLogger LOG = Loggers.forClass(IndexBuildTask.class);

    private static final PartitionReplicationMessagesFactory TABLE_MESSAGES_FACTORY = new PartitionReplicationMessagesFactory();

    private final IndexBuildTaskId taskId;

    private final IndexStorage indexStorage;

    private final MvPartitionStorage partitionStorage;

    private final ReplicaService replicaService;

    private final Executor executor;

    private final IgniteSpinBusyLock busyLock;

    private final int batchSize;

    private final ClusterNode node;

    private final List<IndexBuildCompletionListener> listeners;

    private final long enlistmentConsistencyToken;

    private final int creationCatalogVersion;

    private final boolean afterDisasterRecovery;

    private final IgniteSpinBusyLock taskBusyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean taskStopGuard = new AtomicBoolean();

    private final CompletableFuture<Void> taskFuture = new CompletableFuture<>();

    IndexBuildTask(
            IndexBuildTaskId taskId,
            IndexStorage indexStorage,
            MvPartitionStorage partitionStorage,
            ReplicaService replicaService,
            Executor executor,
            IgniteSpinBusyLock busyLock,
            int batchSize,
            ClusterNode node,
            List<IndexBuildCompletionListener> listeners,
            long enlistmentConsistencyToken,
            int creationCatalogVersion,
            boolean afterDisasterRecovery
    ) {
        this.taskId = taskId;
        this.indexStorage = indexStorage;
        this.partitionStorage = partitionStorage;
        this.replicaService = replicaService;
        this.executor = executor;
        this.busyLock = busyLock;
        this.batchSize = batchSize;
        this.node = node;
        // We do not intentionally make a copy of the list, we want to see changes in the passed list.
        this.listeners = listeners;
        this.enlistmentConsistencyToken = enlistmentConsistencyToken;
        this.creationCatalogVersion = creationCatalogVersion;
        this.afterDisasterRecovery = afterDisasterRecovery;
    }

    /** Starts building the index. */
    void start() {
        if (!enterBusy()) {
            taskFuture.complete(null);

            return;
        }

        LOG.info("Start building the index: [{}]", createCommonIndexInfo());

        try {
            supplyAsync(this::handleNextBatch, executor)
                    .thenCompose(Function.identity())
                    .whenComplete((unused, throwable) -> {
                        if (throwable != null) {
                            if (unwrapCause(throwable) instanceof PrimaryReplicaMissException) {
                                LOG.debug("Index build error: [{}]", throwable, createCommonIndexInfo());
                            } else {
                                LOG.error("Index build error: [{}]", throwable, createCommonIndexInfo());
                            }

                            taskFuture.completeExceptionally(throwable);
                        } else {
                            taskFuture.complete(null);
                        }
                    });
        } catch (Throwable t) {
            taskFuture.completeExceptionally(t);

            throw t;
        } finally {
            leaveBusy();
        }
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

    private CompletableFuture<Void> handleNextBatch() {
        if (!enterBusy()) {
            return nullCompletedFuture();
        }

        try {
            List<RowId> batchRowIds = createBatchRowIds();

            return replicaService.invoke(node, createBuildIndexReplicaRequest(batchRowIds))
                    .handleAsync((unused, throwable) -> {
                        if (throwable != null) {
                            Throwable cause = unwrapCause(throwable);

                            // Read-write transaction operations have not yet completed, let's try to send the batch again.
                            if (!(cause instanceof ReplicationTimeoutException)) {
                                return CompletableFuture.<Void>failedFuture(cause);
                            }
                        } else if (indexStorage.getNextRowIdToBuild() == null) {
                            // Index has been built.
                            LOG.info("Index build completed: [{}]", createCommonIndexInfo());

                            notifyListeners(taskId);

                            return CompletableFutures.<Void>nullCompletedFuture();
                        }

                        return handleNextBatch();
                    }, executor)
                    .thenCompose(Function.identity());
        } catch (Throwable t) {
            return failedFuture(t);
        } finally {
            leaveBusy();
        }
    }

    private List<RowId> createBatchRowIds() {
        RowId nextRowIdToBuild = indexStorage.getNextRowIdToBuild();

        List<RowId> batch = new ArrayList<>(batchSize);

        for (int i = 0; i < batchSize && nextRowIdToBuild != null; i++) {
            nextRowIdToBuild = partitionStorage.closestRowId(nextRowIdToBuild);

            if (nextRowIdToBuild == null) {
                break;
            }

            batch.add(nextRowIdToBuild);

            nextRowIdToBuild = nextRowIdToBuild.increment();
        }

        return batch;
    }

    private BuildIndexReplicaRequest createBuildIndexReplicaRequest(List<RowId> rowIds) {
        boolean finish = rowIds.size() < batchSize;

        return TABLE_MESSAGES_FACTORY.buildIndexReplicaRequest()
                .groupId(new TablePartitionId(taskId.getTableId(), taskId.getPartitionId()))
                .indexId(taskId.getIndexId())
                .rowIds(rowIds.stream().map(RowId::uuid).collect(toList()))
                .finish(finish)
                .enlistmentConsistencyToken(enlistmentConsistencyToken)
                .creationCatalogVersion(creationCatalogVersion)
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
                "tableId={}, partitionId={}, indexId={}",
                taskId.getTableId(), taskId.getPartitionId(), taskId.getIndexId()
        );
    }

    private void notifyListeners(IndexBuildTaskId taskId) {
        for (IndexBuildCompletionListener listener : listeners) {
            if (afterDisasterRecovery) {
                listener.onBuildCompletionAfterDisasterRecovery(taskId.getIndexId(), taskId.getTableId(), taskId.getPartitionId());
            } else {
                listener.onBuildCompletion(taskId.getIndexId(), taskId.getTableId(), taskId.getPartitionId());
            }
        }
    }
}
