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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.RemoveIndexEventParameters;
import org.apache.ignite.internal.catalog.events.StoppingIndexEventParameters;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

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

    private final ChangeIndexStatusTaskScheduler changeIndexStatusTaskScheduler;

    /** Tables IDs for which the local node is the primary replica for the partition with ID {@code 0}. */
    private final Set<Integer> localNodeIsPrimaryReplicaForTableIds = ConcurrentHashMap.newKeySet();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    ChangeIndexStatusTaskController(
            CatalogManager catalogManager,
            PlacementDriver placementDriver,
            ClusterService clusterService,
            ChangeIndexStatusTaskScheduler changeIndexStatusTaskScheduler
    ) {
        this.catalogService = catalogManager;
        this.placementDriver = placementDriver;
        this.clusterService = clusterService;
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

        placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, EventListener.fromConsumer(this::onPrimaryReplicaElected));
    }

    private void onIndexCreated(CreateIndexEventParameters parameters) {
        inBusyLock(busyLock, () -> {
            CatalogIndexDescriptor indexDescriptor = parameters.indexDescriptor();

            if (localNodeIsPrimaryReplicaForTableIds.contains(indexDescriptor.tableId())) {
                // Schedule building the index only if the local node is the primary replica for the 0 partition of the table for which the
                // index was created.
                changeIndexStatusTaskScheduler.scheduleStartBuildingTask(parameters.indexDescriptor());
            }
        });
    }

    private void onIndexDropped(StoppingIndexEventParameters parameters) {
        inBusyLock(busyLock, () -> {
            CatalogIndexDescriptor indexDescriptor = catalogService.index(parameters.indexId(), parameters.catalogVersion());

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

    private void onPrimaryReplicaElected(PrimaryReplicaEventParameters parameters) {
        inBusyLock(busyLock, () -> {
            TablePartitionId primaryReplicaId = (TablePartitionId) parameters.groupId();

            if (primaryReplicaId.partitionId() != 0) {
                // We are only interested in the 0 partition.
                return;
            }

            int tableId = primaryReplicaId.tableId();

            if (isLocalNode(clusterService, parameters.leaseholderId())) {
                if (localNodeIsPrimaryReplicaForTableIds.add(tableId)) {
                    scheduleTasksOnPrimaryReplicaElectedBusy(tableId);
                }
            } else {
                if (localNodeIsPrimaryReplicaForTableIds.remove(tableId)) {
                    changeIndexStatusTaskScheduler.stopTasksForTable(tableId);
                }
            }
        });
    }

    private void scheduleTasksOnPrimaryReplicaElectedBusy(int tableId) {
        // It is safe to get the latest version of the catalog because the PRIMARY_REPLICA_ELECTED event is handled on the metastore thread.
        int catalogVersion = catalogService.latestCatalogVersion();

        for (CatalogIndexDescriptor indexDescriptor : catalogService.indexes(catalogVersion, tableId)) {
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
    }
}
