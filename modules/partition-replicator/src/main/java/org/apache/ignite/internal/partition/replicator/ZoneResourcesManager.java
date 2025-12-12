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

package org.apache.ignite.internal.partition.replicator;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.raft.ZonePartitionRaftListener;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.LogStorageAccessImpl;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionTxStateAccessImpl;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.ZonePartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.storage.state.ThreadAssertingTxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbSharedStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbStorage;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.SafeTimeValuesTracker;
import org.apache.ignite.internal.worker.ThreadAssertions;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Manages resources of distribution zones; that is, allows creation of underlying storages and closes them on node stop.
 */
public class ZoneResourcesManager implements ManuallyCloseable {
    private final TxStateRocksDbSharedStorage sharedTxStateStorage;

    private final TxManager txManager;

    private final OutgoingSnapshotsManager outgoingSnapshotsManager;

    private final TopologyService topologyService;

    private final CatalogService catalogService;

    private final FailureProcessor failureProcessor;

    private final Executor partitionOperationsExecutor;

    private final ReplicaManager replicaManager;

    /** Map from zone IDs to their resource holders. */
    private final Map<Integer, ZoneResources> resourcesByZoneId = new ConcurrentHashMap<>();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    ZoneResourcesManager(
            TxStateRocksDbSharedStorage sharedTxStateStorage,
            TxManager txManager,
            OutgoingSnapshotsManager outgoingSnapshotsManager,
            TopologyService topologyService,
            CatalogService catalogService,
            FailureProcessor failureProcessor,
            Executor partitionOperationsExecutor,
            ReplicaManager replicaManager
    ) {
        this.sharedTxStateStorage = sharedTxStateStorage;
        this.txManager = txManager;
        this.outgoingSnapshotsManager = outgoingSnapshotsManager;
        this.topologyService = topologyService;
        this.catalogService = catalogService;
        this.failureProcessor = failureProcessor;
        this.partitionOperationsExecutor = partitionOperationsExecutor;
        this.replicaManager = replicaManager;
    }

    ZonePartitionResources allocateZonePartitionResources(
            ZonePartitionId zonePartitionId,
            int partitionCount,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker
    ) {
        ZoneResources zoneResources = resourcesByZoneId.computeIfAbsent(
                zonePartitionId.zoneId(),
                zoneId -> new ZoneResources(createTxStateStorage(zoneId, partitionCount))
        );

        TxStatePartitionStorage txStatePartitionStorage = zoneResources.txStateStorage
                .getOrCreatePartitionStorage(zonePartitionId.partitionId());

        var safeTimeTracker = new SafeTimeValuesTracker(HybridTimestamp.MIN_VALUE);

        var raftGroupListener = new ZonePartitionRaftListener(
                zonePartitionId,
                txStatePartitionStorage,
                txManager,
                safeTimeTracker,
                storageIndexTracker,
                outgoingSnapshotsManager,
                partitionOperationsExecutor
        );

        var snapshotStorage = new PartitionSnapshotStorage(
                new ZonePartitionKey(zonePartitionId.zoneId(), zonePartitionId.partitionId()),
                topologyService,
                outgoingSnapshotsManager,
                new PartitionTxStateAccessImpl(txStatePartitionStorage),
                catalogService,
                failureProcessor,
                partitionOperationsExecutor,
                new LogStorageAccessImpl(replicaManager)
        );

        var zonePartitionResources = new ZonePartitionResources(
                txStatePartitionStorage,
                raftGroupListener,
                snapshotStorage,
                storageIndexTracker
        );

        zoneResources.resourcesByPartitionId.put(zonePartitionId.partitionId(), zonePartitionResources);

        return zonePartitionResources;
    }

    @Nullable ZonePartitionResources getZonePartitionResources(ZonePartitionId zonePartitionId) {
        ZoneResources zoneResources = resourcesByZoneId.get(zonePartitionId.zoneId());

        if (zoneResources == null) {
            return null;
        }

        return zoneResources.resourcesByPartitionId.get(zonePartitionId.partitionId());
    }

    protected TxStateStorage createTxStateStorage(int zoneId, int partitionCount) {
        TxStateStorage txStateStorage = new TxStateRocksDbStorage(zoneId, partitionCount, sharedTxStateStorage);

        if (ThreadAssertions.enabled()) {
            txStateStorage = new ThreadAssertingTxStateStorage(txStateStorage);
        }

        txStateStorage.start();

        return txStateStorage;
    }

    @Override
    public void close() {
        busyLock.block();

        for (ZoneResources zoneResources : resourcesByZoneId.values()) {
            zoneResources.txStateStorage.close();
            zoneResources.resourcesByPartitionId.clear();
        }

        resourcesByZoneId.clear();
    }

    void destroyZonePartitionResources(ZonePartitionId zonePartitionId) {
        inBusyLock(busyLock, () -> {
            ZoneResources resources = resourcesByZoneId.get(zonePartitionId.zoneId());

            if (resources != null) {
                resources.resourcesByPartitionId.remove(zonePartitionId.partitionId());

                resources.txStateStorage.destroyPartitionStorage(zonePartitionId.partitionId());
            }
        });
    }

    CompletableFuture<Void> removeTableResources(ZonePartitionId zonePartitionId, int tableId) {
        ZonePartitionResources resources = getZonePartitionResources(zonePartitionId);

        if (resources == null) {
            return nullCompletedFuture();
        }

        return resources.replicaListenerFuture
                .thenCompose(zoneReplicaListener -> {
                    zoneReplicaListener.removeTableReplicaProcessor(tableId);

                    resources.raftListener().removeTableProcessor(tableId);

                    return resources.snapshotStorage().removeMvPartition(tableId);
                });
    }

    @TestOnly
    @Nullable
    TxStatePartitionStorage txStatePartitionStorage(int zoneId, int partitionId) {
        ZoneResources resources = resourcesByZoneId.get(zoneId);

        if (resources == null) {
            return null;
        }

        return resources.txStateStorage.getPartitionStorage(partitionId);
    }

    private static class ZoneResources {

        final TxStateStorage txStateStorage;

        final Map<Integer, ZonePartitionResources> resourcesByPartitionId = new ConcurrentHashMap<>();

        ZoneResources(TxStateStorage txStateStorage) {
            this.txStateStorage = txStateStorage;
        }
    }

    /**
     * Zone partition resources.
     */
    public static class ZonePartitionResources {
        private final TxStatePartitionStorage txStatePartitionStorage;

        private final ZonePartitionRaftListener raftListener;

        private final PartitionSnapshotStorage snapshotStorage;

        private final PendingComparableValuesTracker<Long, Void> storageIndexTracker;

        /**
         * Future that completes when the zone-wide replica listener is created.
         *
         * <p>This is needed, because on recovery tables are started before zone replicas and we need to postpone registering table-wide
         * replica listeners until the zone replica is started.
         *
         * <p>During normal operations this future will be complete and table-wide listener registration will happen immediately.
         */
        private final CompletableFuture<ZonePartitionReplicaListener> replicaListenerFuture = new CompletableFuture<>();

        ZonePartitionResources(
                TxStatePartitionStorage txStatePartitionStorage,
                ZonePartitionRaftListener raftListener,
                PartitionSnapshotStorage snapshotStorage,
                PendingComparableValuesTracker<Long, Void> storageIndexTracker
        ) {
            this.txStatePartitionStorage = txStatePartitionStorage;
            this.raftListener = raftListener;
            this.snapshotStorage = snapshotStorage;
            this.storageIndexTracker = storageIndexTracker;
        }

        public TxStatePartitionStorage txStatePartitionStorage() {
            return txStatePartitionStorage;
        }

        public boolean txStatePartitionStorageIsInRebalanceState() {
            return txStatePartitionStorage.lastAppliedIndex() == TxStatePartitionStorage.REBALANCE_IN_PROGRESS;
        }

        public ZonePartitionRaftListener raftListener() {
            return raftListener;
        }

        public PartitionSnapshotStorage snapshotStorage() {
            return snapshotStorage;
        }

        public PendingComparableValuesTracker<Long, Void> storageIndexTracker() {
            return storageIndexTracker;
        }

        public CompletableFuture<ZonePartitionReplicaListener> replicaListenerFuture() {
            return replicaListenerFuture;
        }
    }
}
