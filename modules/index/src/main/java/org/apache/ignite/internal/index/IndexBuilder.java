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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.command.BuildIndexCommand;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterService;

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
            buildIndexExecutor.submit(new BuildIndexTask(table, tableIndexView, partitionId, true));
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

        private final boolean firstBatch;

        private BuildIndexTask(TableImpl table, TableIndexView tableIndexView, int partitionId, boolean firstBatch) {
            this.table = table;
            this.tableIndexView = tableIndexView;
            this.partitionId = partitionId;
            this.firstBatch = firstBatch;
        }

        @Override
        public void run() {
            if (!busyLock.enterBusy()) {
                return;
            }

            try {
                InternalTable internalTable = table.internalTable();

                RaftGroupService raftGroupService = internalTable.partitionRaftGroupService(partitionId);

                if (!isLocalNodeLeader(raftGroupService)) {
                    // TODO: IGNITE-19053 Must handle the change of leader
                    return;
                }

                RowId lastBuildRowId = internalTable.storage().getOrCreateIndex(partitionId, tableIndexView.id()).getLastBuildRowId();

                if (lastBuildRowId == null) {
                    // Index has already been built.
                    return;
                }

                if (firstBatch) {
                    LOG.info("Start building the index: [{}]", createCommonTableIndexInfo());
                }

                List<RowId> batchRowIds = createBatchRowIds(lastBuildRowId, BUILD_INDEX_ROW_ID_BATCH_SIZE);

                boolean finish = batchRowIds.size() < BUILD_INDEX_ROW_ID_BATCH_SIZE;

                raftGroupService.run(createBuildIndexCommand(batchRowIds, finish))
                        .thenRun(() -> {
                            if (!finish) {
                                buildIndexExecutor.submit(new BuildIndexTask(table, tableIndexView, partitionId, false));
                            }
                        });
            } catch (Throwable t) {
                LOG.error("Index build error: [{}]", t, createCommonTableIndexInfo());
            } finally {
                busyLock.leaveBusy();
            }
        }

        private boolean isLocalNodeLeader(RaftGroupService raftGroupService) {
            Peer leader = raftGroupService.leader();

            assert leader != null : "tableId=" + table.tableId() + ", partitionId=" + partitionId;

            return localNodeConsistentId().equals(leader.consistentId());
        }

        private List<RowId> createBatchRowIds(RowId lastBuildRowId, int batchSize) {
            MvPartitionStorage mvPartition = table.internalTable().storage().getMvPartition(partitionId);

            assert mvPartition != null : createCommonTableIndexInfo();

            List<RowId> batch = new ArrayList<>(batchSize);

            for (int i = 0; i < batchSize && lastBuildRowId != null; i++) {
                lastBuildRowId = mvPartition.closestRowId(lastBuildRowId);

                if (lastBuildRowId == null) {
                    break;
                }

                batch.add(lastBuildRowId);

                lastBuildRowId = lastBuildRowId.increment();
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
    }
}
