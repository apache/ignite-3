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
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.REGISTERED;
import static org.apache.ignite.internal.index.IndexManagementUtils.isLocalNode;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.ClockWaiter;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.DropIndexEventParameters;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterService;

/**
 * Component is responsible for starting and stopping {@link IndexBuildingStarterTask}.
 *
 * <p>Tasks will be started only if the local node is the primary replica for the {@code 0} partition of the table for which indexes are
 * created.</p>
 *
 * <br><p>Tasks are started and stopped based on events:</p>
 * <ul>
 *     <li>{@link CatalogEvent#INDEX_CREATE} - task starts for the new index.</li>
 *     <li>{@link CatalogEvent#INDEX_DROP} - stops task for the new index if it has been added.</li>
 *     <li>{@link PrimaryReplicaEvent#PRIMARY_REPLICA_ELECTED} - when the local node becomes the primary replica for partition {@code 0} of
 *     the table, it starts tasks for it all new indexes, and when the primary replica expires, it stops all tasks for the table.</li>
 * </ul>
 *
 * <br><p>On node recovery, tasks will be started on {@link PrimaryReplicaEvent#PRIMARY_REPLICA_ELECTED}, which will fire due to a change in
 * the {@link ReplicaMeta#getLeaseholderId()} on node restart but after {@link ReplicaMeta#getExpirationTime()}.</p>
 */
// TODO: IGNITE-21115 документация
class IndexBuildingStarter implements ManuallyCloseable {
    private final CatalogManager catalogManager;

    private final ClusterService clusterService;

    private final ClockWaiter clockWaiter;

    private final PlacementDriver placementDriver;

    /** Tables IDs for which the local node is the primary replica for the partition with ID {@code 0}. */
    private final Set<Integer> tableIds = ConcurrentHashMap.newKeySet();

    private final Map<IndexBuildingStarterTaskId, IndexBuildingStarterTask> taskById = new ConcurrentHashMap<>();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    IndexBuildingStarter(
            CatalogManager catalogManager,
            ClusterService clusterService,
            ClockWaiter clockWaiter,
            PlacementDriver placementDriver
    ) {
        this.catalogManager = catalogManager;
        this.clusterService = clusterService;
        this.clockWaiter = clockWaiter;
        this.placementDriver = placementDriver;

        addListeners();
    }

    @Override
    public void close() throws Exception {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        stopAllTasks();
    }

    private void addListeners() {
        catalogManager.listen(CatalogEvent.INDEX_CREATE, (parameters, exception) -> {
            if (exception != null) {
                return failedFuture(exception);
            }

            return onIndexCreate((CreateIndexEventParameters) parameters).thenApply(unused -> false);
        });

        catalogManager.listen(CatalogEvent.INDEX_DROP, (parameters, exception) -> {
            if (exception != null) {
                return failedFuture(exception);
            }

            return onIndexDrop((DropIndexEventParameters) parameters).thenApply(unused -> false);
        });

        placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, (parameters, exception) -> {
            if (exception != null) {
                return failedFuture(exception);
            }

            return onPrimaryReplicaElected(parameters).thenApply(unused -> false);
        });
    }

    private CompletableFuture<?> onIndexCreate(CreateIndexEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            CatalogIndexDescriptor indexDescriptor = parameters.indexDescriptor();

            if (tableIds.contains(indexDescriptor.tableId())) {
                // Start building the index only if the local node is the primary replica for the 0 partition of the table for which the
                // index was created.
                tryScheduleTaskBusy(indexDescriptor);
            }

            return nullCompletedFuture();
        });
    }

    private CompletableFuture<?> onIndexDrop(DropIndexEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            IndexBuildingStarterTask removed = taskById.remove(new IndexBuildingStarterTaskId(parameters.tableId(), parameters.indexId()));

            if (removed != null) {
                removed.stop();
            }

            return nullCompletedFuture();
        });
    }

    private CompletableFuture<?> onPrimaryReplicaElected(PrimaryReplicaEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            TablePartitionId primaryReplicaId = (TablePartitionId) parameters.groupId();

            if (primaryReplicaId.partitionId() != 0) {
                // We are only interested in the 0 partition.
                return nullCompletedFuture();
            }

            int tableId = primaryReplicaId.tableId();

            if (isLocalNode(clusterService, parameters.leaseholderId())) {
                if (tableIds.add(tableId)) {
                    scheduleTasksOnPrimaryReplicaElectedBusy(tableId);
                }
            } else {
                if (tableIds.remove(tableId)) {
                    stopTasksOnPrimaryReplicaElectedBusy(tableId);
                }
            }

            return nullCompletedFuture();
        });
    }

    private void tryScheduleTaskBusy(CatalogIndexDescriptor indexDescriptor) {
        IndexBuildingStarterTaskId taskId = new IndexBuildingStarterTaskId(indexDescriptor.tableId(), indexDescriptor.id());

        IndexBuildingStarterTask task = new IndexBuildingStarterTask();

        if (taskById.putIfAbsent(taskId, task) != null) {
            // Task has already been added and is running.
            return;
        }

        task.start().whenComplete((unused, throwable) -> taskById.remove(taskId));
    }

    private void scheduleTasksOnPrimaryReplicaElectedBusy(int tableId) {
        // It is safe to get the latest version of the catalog because the PRIMARY_REPLICA_ELECTED event is handled on the metastore thread.
        int catalogVersion = catalogManager.latestCatalogVersion();

        for (CatalogIndexDescriptor indexDescriptor : catalogManager.indexes(catalogVersion, tableId)) {
            if (indexDescriptor.status() == REGISTERED) {
                tryScheduleTaskBusy(indexDescriptor);
            }
        }
    }

    private void stopTasksOnPrimaryReplicaElectedBusy(int tableId) {
        Iterator<Entry<IndexBuildingStarterTaskId, IndexBuildingStarterTask>> it = taskById.entrySet().iterator();

        while (it.hasNext()) {
            Entry<IndexBuildingStarterTaskId, IndexBuildingStarterTask> e = it.next();

            if (e.getKey().tableId() == tableId) {
                it.remove();

                e.getValue().stop();
            }
        }
    }

    private void stopAllTasks() {
        taskById.values().forEach(IndexBuildingStarterTask::stop);
        taskById.clear();
    }
}
