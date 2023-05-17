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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.distributed.command.BuildIndexCommand;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Class for managing the building of table indexes.
 */
public class IndexBuilder implements ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(IndexBuilder.class);

    private static final int BATCH_SIZE = 100;

    private final ExecutorService executor;

    private final Map<IndexBuildTaskId, IndexBuildTask> indexBuildTaskById = new ConcurrentHashMap<>();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param threadCount Number of threads to build indexes.
     */
    public IndexBuilder(String nodeName, int threadCount) {
        executor = new ThreadPoolExecutor(
                threadCount,
                threadCount,
                30,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                NamedThreadFactory.create(nodeName, "build-index", LOG)
        );
    }

    /**
     * Starts building the index if it is not already built or is not yet in progress.
     *
     * <p>Index is built in batches using {@link BuildIndexCommand} (via raft), batches are sent sequentially.
     *
     * <p>It is expected that the index building is triggered by the leader of the raft group.
     *
     * @param tableId Table ID.
     * @param partitionId Partition ID.
     * @param indexId Index ID.
     * @param indexStorage Index storage to build.
     * @param partitionStorage Multi-versioned partition storage.
     * @param raftClient Raft client.
     */
    public void startBuildIndex(
            UUID tableId,
            int partitionId,
            UUID indexId,
            IndexStorage indexStorage,
            MvPartitionStorage partitionStorage,
            RaftGroupService raftClient
    ) {
        inBusyLock(() -> {
            if (indexStorage.getNextRowIdToBuild() == null) {
                return;
            }

            IndexBuildTaskId taskId = new IndexBuildTaskId(tableId, partitionId, indexId);

            IndexBuildTask newTask = new IndexBuildTask(taskId, indexStorage, partitionStorage, raftClient, executor, busyLock, BATCH_SIZE);

            IndexBuildTask currentTask = indexBuildTaskById.putIfAbsent(taskId, newTask);

            if (currentTask != newTask) {
                // Index building is already in progress.
                return;
            }

            currentTask.start();

            currentTask.getTaskFuture().whenComplete((unused, throwable) -> indexBuildTaskById.remove(taskId));
        });
    }

    /**
     * Stops index building if it is in progress.
     */
    public void stopBuildIndex(UUID tableId, int partitionId, UUID indexId) {
        inBusyLock(() -> {
            IndexBuildTask removed = indexBuildTaskById.remove(new IndexBuildTaskId(tableId, partitionId, indexId));

            if (removed != null) {
                removed.stop();
            }
        });
    }

    @Override
    public void close() {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
    }

    private void inBusyLock(Runnable runnable) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            runnable.run();
        } finally {
            busyLock.leaveBusy();
        }
    }
}
