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

import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockSafe;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.distributed.command.BuildIndexCommand;
import org.apache.ignite.internal.table.distributed.replication.request.BuildIndexReplicaRequest;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;

/**
 * Class for managing the building of table indexes.
 */
public class IndexBuilder implements ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(IndexBuilder.class);

    private static final int BATCH_SIZE = 100;

    private final ExecutorService executor;

    private final ReplicaService replicaService;

    private final Map<IndexBuildTaskId, IndexBuildTask> indexBuildTaskById = new ConcurrentHashMap<>();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param threadCount Number of threads to build indexes.
     * @param replicaService Replica service.
     */
    public IndexBuilder(String nodeName, int threadCount, ReplicaService replicaService) {
        this.replicaService = replicaService;

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
     * Schedules building the index if it is not already built or is not yet in progress.
     *
     * <p>Index is built in batches using {@link BuildIndexReplicaRequest}, which are then transformed into {@link BuildIndexCommand} on the
     * replica, batches are sent sequentially.</p>
     *
     * <p>It is expected that the index building is triggered by the primary replica.</p>
     *
     * @param tableId Table ID.
     * @param partitionId Partition ID.
     * @param indexId Index ID.
     * @param indexStorage Index storage to build.
     * @param partitionStorage Multi-versioned partition storage.
     * @param node Node to which requests to build the index will be sent.
     */
    // TODO: IGNITE-19498 Perhaps we need to start building the index only once
    public void scheduleBuildIndex(
            int tableId,
            int partitionId,
            int indexId,
            IndexStorage indexStorage,
            MvPartitionStorage partitionStorage,
            ClusterNode node
    ) {
        inBusyLockSafe(busyLock, () -> {
            if (indexStorage.getNextRowIdToBuild() == null) {
                return;
            }

            IndexBuildTaskId taskId = new IndexBuildTaskId(tableId, partitionId, indexId);

            IndexBuildTask newTask = new IndexBuildTask(
                    taskId,
                    indexStorage,
                    partitionStorage,
                    replicaService,
                    executor,
                    busyLock,
                    BATCH_SIZE,
                    node
            );

            IndexBuildTask previousTask = indexBuildTaskById.putIfAbsent(taskId, newTask);

            if (previousTask != null) {
                // Index building is already in progress.
                return;
            }

            newTask.start();

            newTask.getTaskFuture().whenComplete((unused, throwable) -> indexBuildTaskById.remove(taskId));
        });
    }

    /**
     * Stops index building if it is in progress.
     *
     * @param tableId Table ID.
     * @param partitionId Partition ID.
     * @param indexId Index ID.
     */
    public void stopBuildIndex(int tableId, int partitionId, int indexId) {
        inBusyLockSafe(busyLock, () -> {
            IndexBuildTask removed = indexBuildTaskById.remove(new IndexBuildTaskId(tableId, partitionId, indexId));

            if (removed != null) {
                removed.stop();
            }
        });
    }

    /**
     * Stops building all indexes (for a table partition) if they are in progress.
     *
     * @param tableId Table ID.
     * @param partitionId Partition ID.
     */
    public void stopBuildingIndexes(int tableId, int partitionId) {
        stopBuildingIndexes(taskId -> tableId == taskId.getTableId() && partitionId == taskId.getPartitionId());
    }

    /**
     * Stops building indexes for all table partition if they are in progress.
     *
     * @param indexId Index ID.
     */
    public void stopBuildingIndexes(int indexId) {
        stopBuildingIndexes(taskId -> indexId == taskId.getIndexId());
    }

    private void stopBuildingIndexes(Predicate<IndexBuildTaskId> stopBuildIndexPredicate) {
        for (Iterator<Entry<IndexBuildTaskId, IndexBuildTask>> it = indexBuildTaskById.entrySet().iterator(); it.hasNext(); ) {
            inBusyLockSafe(busyLock, () -> {
                Entry<IndexBuildTaskId, IndexBuildTask> entry = it.next();

                if (stopBuildIndexPredicate.test(entry.getKey())) {
                    it.remove();

                    entry.getValue().stop();
                }
            });
        }
    }

    @Override
    public void close() {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
    }
}
