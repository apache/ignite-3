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
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.command.BuildIndexCommand;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterService;
import org.jetbrains.annotations.Nullable;

/**
 * Class for managing the index building process.
 */
class IndexBuilder {
    private static final IgniteLogger LOG = Loggers.forClass(IndexBuilder.class);

    /** Batch size of row IDs to build the index. */
    private static final int BUILD_INDEX_ROW_ID_BATCH_SIZE = 100;

    /** Message factory to create messages - RAFT commands. */
    private static final TableMessagesFactory TABLE_MESSAGES_FACTORY = new TableMessagesFactory();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock;

    /** Cluster service. */
    private final ClusterService clusterService;

    /** Index building executor. */
    private final ExecutorService buildIndexExecutor;

    /** Tasks of building indexes by their ID. */
    private final Map<BuildIndexTaskId, BuildIndexTask> buildIndexTaskById = new ConcurrentHashMap<>();

    IndexBuilder(String nodeName, IgniteSpinBusyLock busyLock, ClusterService clusterService) {
        this.busyLock = busyLock;
        this.clusterService = clusterService;

        int cpus = Runtime.getRuntime().availableProcessors();

        buildIndexExecutor = new ThreadPoolExecutor(
                cpus,
                cpus,
                30,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                NamedThreadFactory.create(nodeName, "build-index", LOG)
        );
    }

    /**
     * Stops the index builder.
     */
    void stop() {
        shutdownAndAwaitTermination(buildIndexExecutor, 10, TimeUnit.SECONDS);
    }

    /**
     * Initializes the build of the index.
     */
    void startIndexBuild(TableIndexView tableIndexView, TableImpl table) {
        for (int partitionId = 0; partitionId < table.internalTable().partitions(); partitionId++) {
            // TODO: IGNITE-19112 We only need to create the index store once
            table.internalTable().storage().getOrCreateIndex(partitionId, tableIndexView.id());

            // TODO: IGNITE-19177 Add assignments check
            buildIndexTaskById.compute(
                    new BuildIndexTaskId(table.tableId(), tableIndexView.id(), partitionId),
                    (buildIndexTaskId, previousBuildIndexTask) -> {
                        assert previousBuildIndexTask == null : buildIndexTaskId;

                        BuildIndexTask buildIndexTask = new BuildIndexTask(table, tableIndexView, buildIndexTaskId.getPartitionId(), null);

                        buildIndexExecutor.submit(buildIndexTask);

                        return buildIndexTask;
                    });
        }
    }

    /**
     * Stops the build of the index.
     */
    void stopIndexBuild(TableIndexView tableIndexView, TableImpl table) {
        for (int partitionId = 0; partitionId < table.internalTable().partitions(); partitionId++) {
            BuildIndexTask buildIndexTask = buildIndexTaskById.remove(
                    new BuildIndexTaskId(table.tableId(), tableIndexView.id(), partitionId)
            );

            if (buildIndexTask != null) {
                buildIndexTask.stop();
            }
        }
    }

    /**
     * Task of building a table index for a partition.
     *
     * <p>Only the leader of the raft group will manage the building of the index. Leader sends batches of row IDs via
     * {@link BuildIndexCommand}, the next batch will only be send after the previous batch has been processed.
     *
     * <p>Index building itself occurs locally on each node of the raft group when processing {@link BuildIndexCommand}. This ensures that
     * the index build process in the raft group is consistent and that the index build process is restored after restarting the raft group
     * (not from the beginning).
     */
    private class BuildIndexTask implements Runnable {
        private final TableImpl table;

        private final TableIndexView tableIndexView;

        private final int partitionId;

        /**
         * ID of the next row to build the index from the previous batch, {@code null} if it is the first row after the index was created
         * (both on a live node and after a restore).
         */
        private final @Nullable RowId nextRowIdToBuildFromPreviousBatch;

        /** Busy lock to stop task synchronously. */
        private final IgniteSpinBusyLock taskBusyLock = new IgniteSpinBusyLock();

        /** Prevents double stopping of the task. */
        private final AtomicBoolean taskStopGuard = new AtomicBoolean();

        private BuildIndexTask(
                TableImpl table,
                TableIndexView tableIndexView,
                int partitionId,
                @Nullable RowId nextRowIdToBuildFromPreviousBatch
        ) {
            this.table = table;
            this.tableIndexView = tableIndexView;
            this.partitionId = partitionId;
            this.nextRowIdToBuildFromPreviousBatch = nextRowIdToBuildFromPreviousBatch;
        }

        @Override
        public void run() {
            completedFuture(null)
                    .thenCompose(unused -> inBusyLock(() -> {
                        // At the time of index creation, table and raft services of all partitions should be already started, so there
                        // should be no errors.
                        RaftGroupService raftGroupService = table.internalTable().partitionRaftGroupService(partitionId);

                        return raftGroupService
                                // Now we do not check the assignment of a node to a partition on purpose, so as not to fight races on the
                                // rebalance, we just wait for the leader of the raft group of the partition, which has had a recovery,
                                // rebalancing (if needed).
                                .refreshAndGetLeaderWithTerm()
                                .thenComposeAsync(leaderWithTerm -> inBusyLock(() -> {
                                    // At this point, we have a stable topology, each node of which has already applied all local updates.
                                    if (!localNodeConsistentId().equals(leaderWithTerm.leader().consistentId())) {
                                        // TODO: IGNITE-19053 Must handle the change of leader
                                        // TODO: IGNITE-19053 Add a test to change the leader even at the start of the task
                                        removeTask();

                                        return completedFuture(null);
                                    }

                                    RowId nextRowIdToBuild = nextRowIdToBuild();

                                    if (nextRowIdToBuild == null) {
                                        // Index has already been built.
                                        removeTask();

                                        return completedFuture(null);
                                    }

                                    List<RowId> batchRowIds = collectRowIdBatch(nextRowIdToBuild);

                                    RowId nextRowId = getNextRowIdForNextBatch(batchRowIds);

                                    boolean finish = batchRowIds.size() < BUILD_INDEX_ROW_ID_BATCH_SIZE || nextRowId == null;

                                    // TODO: IGNITE-19053 Must handle the change of leader
                                    return raftGroupService.run(createBuildIndexCommand(batchRowIds, finish))
                                            .thenRun(() -> inBusyLock(() -> {
                                                scheduleNextBuildIndexTaskIfNeeded(nextRowId, finish);

                                                return completedFuture(null);
                                            }));
                                }), buildIndexExecutor);
                    })).whenComplete((unused, throwable) -> {
                        if (throwable != null) {
                            removeTask();

                            LOG.error("Index build error: [{}]", throwable, createCommonTableIndexInfo());
                        }
                    });
        }

        private void scheduleNextBuildIndexTaskIfNeeded(@Nullable RowId nextRowId, boolean finish) {
            buildIndexTaskById.compute(
                    new BuildIndexTaskId(table.tableId(), tableIndexView.id(), partitionId),
                    (buildIndexTaskId, previousBuildIndexTask) -> {
                        if (previousBuildIndexTask == null || finish) {
                            // Index build task is in the process of stopping or has completed.
                            return null;
                        }

                        assert nextRowId != null : createCommonTableIndexInfo();

                        BuildIndexTask buildIndexTask = new BuildIndexTask(
                                table,
                                tableIndexView,
                                partitionId,
                                nextRowId
                        );

                        buildIndexExecutor.submit(buildIndexTask);

                        return buildIndexTask;
                    });
        }

        private boolean isLocalNodeLeader(RaftGroupService raftGroupService) {
            Peer leader = raftGroupService.leader();

            assert leader != null : "tableId=" + table.tableId() + ", partitionId=" + partitionId;

            return localNodeConsistentId().equals(leader.consistentId());
        }

        private List<RowId> createBatchRowIds(RowId lastBuiltRowId, int batchSize) {
            MvPartitionStorage mvPartition = table.internalTable().storage().getMvPartition(partitionId);

            assert mvPartition != null : createCommonTableIndexInfo();

            List<RowId> batch = new ArrayList<>(batchSize);

            for (int i = 0; i < batchSize && lastBuiltRowId != null; i++) {
                lastBuiltRowId = mvPartition.closestRowId(lastBuiltRowId);

                if (lastBuiltRowId == null) {
                    break;
                }

                batch.add(lastBuiltRowId);

                lastBuiltRowId = lastBuiltRowId.increment();
            }

            return batch;
        }

        private BuildIndexCommand createBuildIndexCommand(List<RowId> rowIds, boolean finish) {
            return TABLE_MESSAGES_FACTORY.buildIndexCommand()
                    .tablePartitionId(TABLE_MESSAGES_FACTORY.tablePartitionIdMessage()
                            .tableId(table.tableId())
                            .partitionId(partitionId)
                            .build()
                    )
                    .indexId(tableIndexView.id())
                    .rowIds(rowIds.stream().map(RowId::uuid).collect(toList()))
                    .finish(finish)
                    .build();
        }

        private String createCommonTableIndexInfo() {
            return "table=" + table.name() + ", tableId=" + table.tableId()
                    + ", partitionId=" + partitionId
                    + ", index=" + tableIndexView.name() + ", indexId=" + tableIndexView.id();
        }

        private String localNodeConsistentId() {
            return clusterService.topologyService().localMember().name();
        }

        private @Nullable RowId getNextRowIdForNextBatch(List<RowId> batch) {
            return batch.isEmpty() ? null : batch.get(batch.size() - 1).increment();
        }

        private @Nullable RowId nextRowIdToBuild() {
            if (nextRowIdToBuildFromPreviousBatch != null) {
                return nextRowIdToBuildFromPreviousBatch;
            }

            IndexStorage index = table.internalTable().storage().getIndex(partitionId, tableIndexView.id());

            assert index != null : createCommonTableIndexInfo();

            return index.getNextRowIdToBuild();
        }

        private List<RowId> collectRowIdBatch(RowId nextRowIdToBuild) {
            if (nextRowIdToBuildFromPreviousBatch == null) {
                LOG.info("Start building the index: [{}]", createCommonTableIndexInfo());
            }

            return createBatchRowIds(nextRowIdToBuild, BUILD_INDEX_ROW_ID_BATCH_SIZE);
        }

        private boolean enterBusy() {
            if (!taskBusyLock.enterBusy()) {
                return false;
            }

            if (!busyLock.enterBusy()) {
                taskBusyLock.leaveBusy();

                return false;
            }

            return true;
        }

        private void leaveBusy() {
            taskBusyLock.leaveBusy();
            busyLock.leaveBusy();
        }

        private <V> CompletableFuture<V> inBusyLock(Supplier<CompletableFuture<V>> supplier) {
            if (!enterBusy()) {
                removeTask();

                return completedFuture(null);
            }

            try {
                return supplier.get();
            } finally {
                leaveBusy();
            }
        }

        private void stop() {
            if (!taskStopGuard.compareAndSet(false, true)) {
                return;
            }

            taskBusyLock.block();
        }

        private void removeTask() {
            buildIndexTaskById.remove(new BuildIndexTaskId(table.tableId(), tableIndexView.id(), partitionId));
        }
    }
}
