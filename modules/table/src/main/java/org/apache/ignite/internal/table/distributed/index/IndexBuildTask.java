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

package org.apache.ignite.internal.table.distributed.index;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.command.BuildIndexCommand;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/**
 * Task of building a table index.
 */
class IndexBuildTask {
    private static final IgniteLogger LOG = Loggers.forClass(IndexBuildTask.class);

    private static final TableMessagesFactory TABLE_MESSAGES_FACTORY = new TableMessagesFactory();

    private final IndexBuildTaskId taskId;

    private final IndexStorage indexStorage;

    private final MvPartitionStorage partitionStorage;

    private final RaftGroupService raftClient;

    private final ExecutorService executor;

    private final IgniteSpinBusyLock busyLock;

    private final int batchSize;

    private final IgniteSpinBusyLock taskBusyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean taskStopGuard = new AtomicBoolean();

    private final CompletableFuture<Void> taskFuture = new CompletableFuture<>();

    IndexBuildTask(
            IndexBuildTaskId taskId,
            IndexStorage indexStorage,
            MvPartitionStorage partitionStorage,
            RaftGroupService raftClient,
            ExecutorService executor,
            IgniteSpinBusyLock busyLock,
            int batchSize
    ) {
        this.taskId = taskId;
        this.indexStorage = indexStorage;
        this.partitionStorage = partitionStorage;
        this.raftClient = raftClient;
        this.executor = executor;
        this.busyLock = busyLock;
        this.batchSize = batchSize;
    }

    /**
     * Starts building the index.
     */
    void start() {
        if (!enterBusy()) {
            taskFuture.complete(null);

            return;
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Start building the index: [{}]", createCommonIndexInfo());
        }

        try {
            supplyAsync(this::handleNextBatch, executor)
                    .thenCompose(Function.identity())
                    .whenComplete((unused, throwable) -> {
                        if (throwable != null) {
                            LOG.error("Index build error: [{}]", throwable, createCommonIndexInfo());

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

    /**
     * Stops index building.
     */
    void stop() {
        if (!taskStopGuard.compareAndSet(false, true)) {
            return;
        }

        taskBusyLock.block();
    }

    /**
     * Returns the index build future.
     */
    CompletableFuture<Void> getTaskFuture() {
        return taskFuture;
    }

    private CompletableFuture<Void> handleNextBatch() {
        if (!enterBusy()) {
            return completedFuture(null);
        }

        try {
            List<RowId> batchRowIds = createBatchRowIds();

            return raftClient.run(createBuildIndexCommand(batchRowIds))
                    .thenComposeAsync(unused -> {
                        if (indexStorage.getNextRowIdToBuild() == null) {
                            // Index has been built.
                            return completedFuture(null);
                        }

                        return handleNextBatch();
                    }, executor);
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

    private BuildIndexCommand createBuildIndexCommand(List<RowId> rowIds) {
        boolean finish = rowIds.size() < batchSize;

        return TABLE_MESSAGES_FACTORY.buildIndexCommand()
                .tablePartitionId(TABLE_MESSAGES_FACTORY.tablePartitionIdMessage()
                        .tableId(taskId.getTableId())
                        .partitionId(taskId.getPartitionId())
                        .build()
                )
                .indexId(taskId.getIndexId())
                .rowIds(rowIds.stream().map(RowId::uuid).collect(toList()))
                .finish(finish)
                .build();
    }

    private boolean enterBusy() {
        if (!busyLock.enterBusy()) {
            return false;
        }

        if (!taskBusyLock.enterBusy()) {
            busyLock.leaveBusy();

            return false;
        }

        return true;
    }

    private void leaveBusy() {
        taskBusyLock.leaveBusy();
        busyLock.leaveBusy();
    }

    private String createCommonIndexInfo() {
        return IgniteStringFormatter.format(
                "tableId={}, partitionId={}, indexId={}",
                taskId.getTableId(), taskId.getPartitionId(), taskId.getIndexId()
        );
    }
}
