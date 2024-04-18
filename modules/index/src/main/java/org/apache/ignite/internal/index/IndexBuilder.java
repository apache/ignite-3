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

import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockSafe;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.distributed.command.BuildIndexCommand;
import org.apache.ignite.internal.table.distributed.replication.request.BuildIndexReplicaRequest;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;

/**
 * Component that is responsible for building an index for a specific partition.
 *
 * <p>Approximate index building algorithm:</p>
 * <ul>
 *     <li>If the index has not yet been built ({@link IndexStorage#getNextRowIdToBuild()} {@code != null}) or is not in the process of
 *     being built, then an asynchronous task is added to build it.</li>
 *     <li>Index building task generates batches of {@link RowId} (by using {@link IndexStorage#getNextRowIdToBuild()}) and sends these
 *     batch to the primary replica (only the primary replica is expected to start building the index) so that the corresponding replication
 *     group builds indexes for the transferred batch.</li>
 *     <li>Subsequent batches will be sent only after the current batch has been processed and until
 *     {@link IndexStorage#getNextRowIdToBuild()} {@code != null}.</li>
 * </ul>
 *
 * <p>Notes: It is expected that only the primary replica will run tasks to build the index, and if the replica loses primacy, it will stop
 * the task to build the index, and this will be done by an external component.</p>
 */
class IndexBuilder implements ManuallyCloseable {
    /** Batch size of row IDs to build the index. */
    static final int BATCH_SIZE = 100;

    private final Executor executor;

    private final ReplicaService replicaService;

    private final Map<IndexBuildTaskId, IndexBuildTask> indexBuildTaskById = new ConcurrentHashMap<>();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    private final List<IndexBuildCompletionListener> listeners = new CopyOnWriteArrayList<>();

    /** Constructor. */
    IndexBuilder(Executor executor, ReplicaService replicaService) {
        this.replicaService = replicaService;
        this.executor = executor;
    }

    /**
     * Schedules building the index if it is not already built or is not yet in progress.
     *
     * <p>Notes:</p>
     * <ul>
     *     <li>Index is built in batches using {@link BuildIndexReplicaRequest}, which are then transformed into {@link BuildIndexCommand}
     *     on the replica, batches are sent sequentially.</li>
     *     <li>It is expected that the index building is triggered by the primary replica.</li>
     *     <li>If the index has already been built, {@link IndexBuildCompletionListener} will be notified.</li>
     * </ul>
     *
     * @param tableId Table ID.
     * @param partitionId Partition ID.
     * @param indexId Index ID.
     * @param indexStorage Index storage to build.
     * @param partitionStorage Multi-versioned partition storage.
     * @param node Node to which requests to build the index will be sent.
     * @param enlistmentConsistencyToken Enlistment consistency token is used to check that the lease is still actual while the message goes
     *      to the replica.
     * @param creationCatalogVersion Catalog version in which the index was created.
     */
    public void scheduleBuildIndex(
            int tableId,
            int partitionId,
            int indexId,
            IndexStorage indexStorage,
            MvPartitionStorage partitionStorage,
            ClusterNode node,
            long enlistmentConsistencyToken,
            int creationCatalogVersion
    ) {
        inBusyLockSafe(busyLock, () -> {
            if (indexStorage.getNextRowIdToBuild() == null) {
                for (IndexBuildCompletionListener listener : listeners) {
                    listener.onBuildCompletion(indexId, tableId, partitionId);
                }

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
                    node,
                    listeners,
                    enlistmentConsistencyToken,
                    creationCatalogVersion
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
    }

    /** Adds a listener. */
    public void listen(IndexBuildCompletionListener listener) {
        listeners.add(listener);
    }

    /** Removes a listener. */
    public void stopListen(IndexBuildCompletionListener listener) {
        listeners.remove(listener);
    }
}
