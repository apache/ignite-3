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
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.index.event.IndexEvent;
import org.apache.ignite.internal.index.event.IndexEventParameters;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.index.IndexBuilder;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;

/** No doc. */
// TODO: IGNITE-20330 код, тесты и документация
public class IndexBuildController implements ManuallyCloseable {
    private final IndexBuilder indexBuilder;

    private final ClusterService clusterService;

    private final TableManager tableManager;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    private final Map<TablePartitionId, MvTableStorage> replicaMvTableStorageById = new ConcurrentHashMap<>();

    /** Constructor. */
    public IndexBuildController(
            IndexBuilder indexBuilder,
            ClusterService clusterService,
            TableManager tableManager,
            IndexManager indexManager,
            PlacementDriver placementDriver
    ) {
        this.indexBuilder = indexBuilder;
        this.clusterService = clusterService;
        this.tableManager = tableManager;

        indexManager.listen(IndexEvent.CREATE, (parameters, exception) -> inBusyLockAsync(busyLock, () -> {
            onIndexCreateBusy(parameters);

            return completedFuture(false);
        }));

        indexManager.listen(IndexEvent.DROP, (parameters, exception) -> inBusyLockAsync(busyLock, () -> {
            onIndexDropBusy(parameters);

            return completedFuture(false);
        }));

        placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, (parameters, exception) -> inBusyLockAsync(busyLock, () -> {
            // TODO: IGNITE-20330 не хватает кода
            return completedFuture(false);
        }));
    }

    @Override
    public void close() throws Exception {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();
    }

    private void onIndexCreateBusy(IndexEventParameters parameters) {
        for (Entry<TablePartitionId, MvTableStorage> entry : replicaMvTableStorageById.entrySet()) {
            TablePartitionId replicaId = entry.getKey();

            int tableId = replicaId.tableId();

            if (tableId == parameters.tableId()) {
                MvTableStorage mvTableStorage = entry.getValue();

                int partitionId = replicaId.partitionId();
                int indexId = parameters.indexId();

                IndexStorage indexStorage = mvTableStorage.getIndex(partitionId, indexId);

                assert indexStorage != null : replicaId;

                MvPartitionStorage mvPartition = mvTableStorage.getMvPartition(partitionId);

                assert mvPartition != null : replicaId;

                indexBuilder.startBuildIndex(tableId, partitionId, indexId, indexStorage, mvPartition, localNode());
            }
        }
    }

    private void onIndexDropBusy(IndexEventParameters parameters) {
        indexBuilder.stopBuildIndexes(parameters.indexId());
    }

    private void onPrimaryReplicaElectedBusy(PrimaryReplicaEventParameters parameters) {
    }

    private ClusterNode localNode() {
        return clusterService.topologyService().localMember();
    }
}
