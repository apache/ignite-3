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

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.RemoveIndexEventParameters;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/**
 * Component is responsible for starting and stopping {@link IndexBuildingStarterTask} via {@link IndexBuildingStarter}.
 *
 * <p>Tasks will be started only if the local node is the primary replica for the {@code 0} partition of the table for which indexes are
 * created.</p>
 *
 * <br><p>Tasks are started and stopped based on events:</p>
 * <ul>
 *     <li>{@link CatalogEvent#INDEX_CREATE} - task starts for the new index.</li>
 *     <li>{@link CatalogEvent#INDEX_REMOVED} - stops task for the new index if it has been added.</li>
 *     <li>{@link PrimaryReplicaEvent#PRIMARY_REPLICA_ELECTED} - when the local node becomes the primary replica for partition {@code 0} of
 *     the table, it starts tasks for its all new indexes, and when the primary replica expires, it stops all tasks for the table.</li>
 * </ul>
 *
 * <br><p>On node recovery, tasks will be started on {@link PrimaryReplicaEvent#PRIMARY_REPLICA_ELECTED}, which will fire due to a change in
 * the {@link ReplicaMeta#getLeaseholderId()} on node restart but after {@link ReplicaMeta#getExpirationTime()}.</p>
 */
class IndexBuildingStarterController implements ManuallyCloseable {
    private final CatalogService catalogService;

    private final PlacementDriver placementDriver;

    private final ClusterService clusterService;

    private final IndexBuildingStarter indexBuildingStarter;

    /** Tables IDs for which the local node is the primary replica for the partition with ID {@code 0}. */
    private final Set<Integer> localNodeIsPrimaryReplicaForTableIds = ConcurrentHashMap.newKeySet();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    IndexBuildingStarterController(
            CatalogService catalogService,
            PlacementDriver placementDriver,
            ClusterService clusterService,
            IndexBuildingStarter indexBuildingStarter
    ) {
        this.catalogService = catalogService;
        this.placementDriver = placementDriver;
        this.clusterService = clusterService;
        this.indexBuildingStarter = indexBuildingStarter;

        addListeners();
    }

    @Override
    public void close() throws Exception {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        indexBuildingStarter.close();
    }

    private void addListeners() {
        catalogService.listen(CatalogEvent.INDEX_CREATE, (parameters, exception) -> {
            if (exception != null) {
                return failedFuture(exception);
            }

            return onIndexCreate((CreateIndexEventParameters) parameters).thenApply(unused -> false);
        });

        catalogService.listen(CatalogEvent.INDEX_REMOVED, (parameters, exception) -> {
            if (exception != null) {
                return failedFuture(exception);
            }

            return onIndexRemoved((RemoveIndexEventParameters) parameters).thenApply(unused -> false);
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

            if (localNodeIsPrimaryReplicaForTableIds.contains(indexDescriptor.tableId())) {
                // Schedule building the index only if the local node is the primary replica for the 0 partition of the table for which the
                // index was created.
                indexBuildingStarter.scheduleTask(parameters.indexDescriptor());
            }

            return nullCompletedFuture();
        });
    }

    private CompletableFuture<?> onIndexRemoved(RemoveIndexEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            CatalogIndexDescriptor indexDescriptor = catalogService.index(parameters.indexId(), parameters.catalogVersion() - 1);

            assert indexDescriptor != null : parameters.indexId();

            indexBuildingStarter.stopTask(indexDescriptor);

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
                if (localNodeIsPrimaryReplicaForTableIds.add(tableId)) {
                    scheduleTasksOnPrimaryReplicaElectedBusy(tableId);
                }
            } else {
                if (localNodeIsPrimaryReplicaForTableIds.remove(tableId)) {
                    indexBuildingStarter.stopTasks(tableId);
                }
            }

            return nullCompletedFuture();
        });
    }

    private void scheduleTasksOnPrimaryReplicaElectedBusy(int tableId) {
        // It is safe to get the latest version of the catalog because the PRIMARY_REPLICA_ELECTED event is handled on the metastore thread.
        int catalogVersion = catalogService.latestCatalogVersion();

        for (CatalogIndexDescriptor indexDescriptor : catalogService.indexes(catalogVersion, tableId)) {
            if (indexDescriptor.status() == REGISTERED) {
                indexBuildingStarter.scheduleTask(indexDescriptor);
            }
        }
    }
}
