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

import static org.apache.ignite.internal.index.IndexManagementUtils.isLocalNode;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntPredicate;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.catalog.events.RemoveIndexEventParameters;
import org.apache.ignite.internal.catalog.events.StoppingIndexEventParameters;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.lowwatermark.event.ChangeLowWatermarkEventParameters;
import org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.LongPriorityQueue;

/**
 * Component that reacts to certain Catalog changes and starts or stops corresponding {@link ChangeIndexStatusTask}s via the
 * {@link ChangeIndexStatusTaskScheduler}.
 *
 * <p>Tasks will be started only if the local node is the primary replica for the {@code 0} partition of the table which indexes have been
 * modified in the Catalog.
 *
 * <p>The following events are being monitored:
 * <ul>
 *     <li>{@link CatalogEvent#INDEX_CREATE} - when an index is created, a task will be started to move the index to the
 *     {@link CatalogIndexStatus#BUILDING} state; </li>
 *     <li>{@link CatalogEvent#INDEX_STOPPING} - when an index is dropped, a task will be started to remove the index from the Catalog;</li>
 *     <li>{@link CatalogEvent#INDEX_REMOVED} - when an index is removed from the Catalog, all ongoing tasks for the index will be stopped;
 *     </li>
 *     <li>{@link PrimaryReplicaEvent#PRIMARY_REPLICA_ELECTED} - if the local node has become the primary replica for partition {@code 0} of
 *     the table, it starts corresponding tasks depending on the current status of indices in the Catalog. If the local node stops being the
 *     primary replica for the partition {@code 0}, it stops all tasks that belong to indices of the table that the partition belongs to.
 *     </li>
 * </ul>
 *
 * <p>On node recovery, tasks will be started on {@link PrimaryReplicaEvent#PRIMARY_REPLICA_ELECTED}, which will fire due to a change in
 * the {@link ReplicaMeta#getLeaseholderId()} on node restart but after {@link ReplicaMeta#getExpirationTime()}.
 */
class ChangeIndexStatusTaskController implements ManuallyCloseable {
    private final CatalogService catalogService;

    private final PlacementDriver placementDriver;

    private final ClusterService clusterService;

    private final LowWatermark lowWatermark;

    private final ChangeIndexStatusTaskScheduler changeIndexStatusTaskScheduler;

    /** Tables IDs for which the local node is the primary replica for the partition with ID {@code 0}. */
    private final Set<Integer> localNodeIsPrimaryReplicaForTableIds = ConcurrentHashMap.newKeySet();

    /** Zone IDs for which the local node is the primary replica for the partition with ID {@code 0}. */
    private final Set<Integer> localNodeIsPrimaryReplicaForZoneIds = ConcurrentHashMap.newKeySet();

    /** A queue for deferred table destruction events. */
    private final LongPriorityQueue<DestroyTableEvent> destructionEventsQueue = new LongPriorityQueue<>(DestroyTableEvent::catalogVersion);

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    ChangeIndexStatusTaskController(
            CatalogManager catalogManager,
            PlacementDriver placementDriver,
            ClusterService clusterService,
            LowWatermark lowWatermark,
            ChangeIndexStatusTaskScheduler changeIndexStatusTaskScheduler
    ) {
        this.catalogService = catalogManager;
        this.placementDriver = placementDriver;
        this.clusterService = clusterService;
        this.lowWatermark = lowWatermark;
        this.changeIndexStatusTaskScheduler = changeIndexStatusTaskScheduler;
    }

    /** Starts component. */
    public void start() {
        inBusyLock(busyLock, this::addListeners);
    }

    @Override
    public void close() throws Exception {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        changeIndexStatusTaskScheduler.close();
    }

    private void addListeners() {
        catalogService.listen(CatalogEvent.INDEX_CREATE, EventListener.fromConsumer(this::onIndexCreated));

        catalogService.listen(CatalogEvent.INDEX_STOPPING, EventListener.fromConsumer(this::onIndexDropped));

        catalogService.listen(CatalogEvent.INDEX_REMOVED, EventListener.fromConsumer(this::onIndexRemoved));

        catalogService.listen(CatalogEvent.TABLE_CREATE, EventListener.fromConsumer(this::onTableCreated));
        catalogService.listen(CatalogEvent.TABLE_DROP, EventListener.fromConsumer(this::onTableDropped));

        lowWatermark.listen(LowWatermarkEvent.LOW_WATERMARK_CHANGED, EventListener.fromConsumer(this::onLwmChanged));

        placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, EventListener.fromConsumer(this::onPrimaryReplicaElected));
    }

    private void onIndexCreated(CreateIndexEventParameters parameters) {
        inBusyLock(busyLock, () -> {
            CatalogIndexDescriptor indexDescriptor = parameters.indexDescriptor();

            if (indexDescriptor.isCreatedWithTable()) {
                // No need to build an index that was created together with its table as its state (empty) already corresponds
                // to the table's state (which is also empty).
                return;
            }

            if (localNodeIsPrimaryReplicaForTableIds.contains(indexDescriptor.tableId())) {
                // Schedule building the index only if the local node is the primary replica for the 0 partition of the table for which the
                // index was created.
                changeIndexStatusTaskScheduler.scheduleStartBuildingTask(parameters.indexDescriptor());
            }
        });
    }

    private void onIndexDropped(StoppingIndexEventParameters parameters) {
        inBusyLock(busyLock, () -> {
            Catalog catalog = catalogService.catalog(parameters.catalogVersion());

            CatalogIndexDescriptor indexDescriptor = catalog.index(parameters.indexId());

            assert indexDescriptor != null : parameters.indexId();

            if (localNodeIsPrimaryReplicaForTableIds.contains(indexDescriptor.tableId())) {
                // Schedule index removal only if the local node is the primary replica for the 0 partition of the table for which the
                // index was dropped.
                changeIndexStatusTaskScheduler.scheduleRemoveIndexTask(indexDescriptor);
            }
        });
    }

    private void onIndexRemoved(RemoveIndexEventParameters parameters) {
        inBusyLock(busyLock, () -> changeIndexStatusTaskScheduler.stopStartBuildingTask(parameters.indexId()));
    }

    private void onTableCreated(CreateTableEventParameters parameters) {
        inBusyLock(busyLock, () -> {
            CatalogTableDescriptor tableDescriptor = parameters.tableDescriptor();

            if (localNodeIsPrimaryReplicaForZoneIds.contains(tableDescriptor.zoneId())) {
                localNodeIsPrimaryReplicaForTableIds.add(tableDescriptor.id());
            }
        });
    }

    private void onTableDropped(DropTableEventParameters parameters) {
        inBusyLock(busyLock, () -> {
            destructionEventsQueue.enqueue(new DestroyTableEvent(parameters.catalogVersion(), parameters.tableId()));
        });
    }

    private void onPrimaryReplicaElected(PrimaryReplicaEventParameters parameters) {
        inBusyLock(busyLock, () -> {

            ZonePartitionId primaryReplicaId = (ZonePartitionId) parameters.groupId();

            if (primaryReplicaId.partitionId() != 0) {
                // We are only interested in the 0 partition.
                return;
            }

            if (isLocalNode(clusterService, parameters.leaseholderId())) {
                scheduleTasksOnPrimaryReplicaElectedBusy(primaryReplicaId);
            } else {
                handlePrimacyLoss(primaryReplicaId);
            }
        });
    }

    private void onLwmChanged(ChangeLowWatermarkEventParameters parameters) {
        int earliestVersion = catalogService.activeCatalogVersion(parameters.newLowWatermark().longValue());
        List<DestroyTableEvent> tablesToDestroy = destructionEventsQueue.drainUpTo(earliestVersion);

        tablesToDestroy.forEach(event -> {
            localNodeIsPrimaryReplicaForTableIds.remove(event.tableId());

            changeIndexStatusTaskScheduler.stopTasksForTable(event.tableId());
        });
    }

    private void scheduleTasksOnPrimaryReplicaElectedBusy(ZonePartitionId zonePartitionId) {
        // It is safe to get the latest version of the catalog because the PRIMARY_REPLICA_ELECTED event is handled on the metastore thread.
        Catalog catalog = catalogService.latestCatalog();

        IntArrayList tableIds =
                getTableIdsForPrimaryReplicaElected(catalog, zonePartitionId, id -> !localNodeIsPrimaryReplicaForTableIds.contains(id));
        localNodeIsPrimaryReplicaForTableIds.addAll(tableIds);

        List<Integer> zoneIds = getZoneIdsForPrimaryReplicaElected(zonePartitionId);
        localNodeIsPrimaryReplicaForZoneIds.addAll(zoneIds);

        tableIds.forEach(tableId -> {
            for (CatalogIndexDescriptor indexDescriptor : catalog.indexes(tableId)) {
                switch (indexDescriptor.status()) {
                    case REGISTERED:
                        changeIndexStatusTaskScheduler.scheduleStartBuildingTask(indexDescriptor);

                        break;

                    case STOPPING:
                        changeIndexStatusTaskScheduler.scheduleRemoveIndexTask(indexDescriptor);

                        break;

                    default:
                        break;
                }
            }
        });
    }

    private void handlePrimacyLoss(ZonePartitionId zonePartitionId) {
        // It is safe to get the latest version of the catalog because the PRIMARY_REPLICA_ELECTED event is handled on the metastore thread.
        Catalog catalog = catalogService.latestCatalog();

        IntArrayList tableIds =
                getTableIdsForPrimaryReplicaElected(catalog, zonePartitionId, localNodeIsPrimaryReplicaForTableIds::contains);

        localNodeIsPrimaryReplicaForTableIds.removeAll(tableIds);
        localNodeIsPrimaryReplicaForZoneIds.remove(zonePartitionId.zoneId());

        tableIds.forEach(changeIndexStatusTaskScheduler::stopTasksForTable);
    }

    private static IntArrayList getTableIdsForPrimaryReplicaElected(
            Catalog catalog,
            ZonePartitionId zonePartitionId,
            IntPredicate predicate
    ) {
        var tableIds = new IntArrayList();

        for (CatalogTableDescriptor table : catalog.tables(zonePartitionId.zoneId())) {
            if (predicate.test(table.id())) {
                tableIds.add(table.id());
            }
        }

        return tableIds;
    }

    private List<Integer> getZoneIdsForPrimaryReplicaElected(ZonePartitionId zonePartitionId) {
        if (!localNodeIsPrimaryReplicaForZoneIds.contains(zonePartitionId.zoneId())) {
            return List.of(zonePartitionId.zoneId());
        } else {
            return List.of();
        }
    }

    private static class DestroyTableEvent {
        final int catalogVersion;
        final int tableId;

        private DestroyTableEvent(int catalogVersion, int tableId) {
            this.catalogVersion = catalogVersion;
            this.tableId = tableId;
        }

        int catalogVersion() {
            return catalogVersion;
        }

        int tableId() {
            return tableId;
        }
    }
}
