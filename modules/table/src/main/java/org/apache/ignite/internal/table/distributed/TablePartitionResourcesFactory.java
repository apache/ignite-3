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

package org.apache.ignite.internal.table.distributed;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionDataStorage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionMvStorageAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;
import org.apache.ignite.internal.placementdriver.LeasePlacementDriver;
import org.apache.ignite.internal.raft.ExecutorInclinedRaftCommandRunner;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.PartitionModificationCounterFactory.SizeSupplier;
import org.apache.ignite.internal.table.distributed.gc.GcUpdateHandler;
import org.apache.ignite.internal.table.distributed.gc.MvGc;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorService;
import org.apache.ignite.internal.table.distributed.raft.TablePartitionProcessor;
import org.apache.ignite.internal.table.distributed.raft.snapshot.FullStateTransferIndexChooser;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionMvStorageAccessImpl;
import org.apache.ignite.internal.table.distributed.raft.snapshot.SnapshotAwarePartitionDataStorage;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionStateResolver;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;

/**
 * Stateless factory for creating partition-level resources: data storage wrappers, update handlers, and replica listeners.
 *
 * <p>This factory performs pure construction only — it does not start components, register metrics,
 * or own any mutable state. Lifecycle management (start/stop, metric registration/deregistration)
 * remains in {@link TableManager}.
 *
 * <p><b>Lifecycle ordering:</b> the caller must invoke {@link StorageUpdateHandler#start} on the
 * {@link PartitionResources#storageUpdateHandler} returned by {@link #createPartitionResources} before
 * the constructed objects ({@link TablePartitionProcessor}, {@link PartitionMvStorageAccess},
 * {@link PartitionReplicaListener}) are used at runtime.
 */
class TablePartitionResourcesFactory {
    private final TxManager txManager;
    private final LockManager lockManager;
    private final ExecutorService scanRequestExecutor;
    private final ClockService clockService;
    private final CatalogService catalogService;
    private final PartitionModificationCounterFactory partitionModificationCounterFactory;
    private final OutgoingSnapshotsManager outgoingSnapshotsManager;
    private final LowWatermark lowWatermark;
    private final ValidationSchemasSource validationSchemasSource;
    private final SchemaSyncService schemaSyncService;
    private final LeasePlacementDriver placementDriver;
    private final TopologyService topologyService;
    private final InternalClusterNode localNode;
    private final RemotelyTriggeredResourceRegistry remotelyTriggeredResourceRegistry;
    private final FailureProcessor failureProcessor;
    private final SchemaManager schemaManager;
    private final ReplicationConfiguration replicationConfiguration;
    private final Executor partitionOperationsExecutor;
    private final IndexMetaStorage indexMetaStorage;
    private final MinimumRequiredTimeCollectorService minTimeCollectorService;
    private final MvGc mvGc;
    private final FullStateTransferIndexChooser fullStateTransferIndexChooser;

    TablePartitionResourcesFactory(
            TxManager txManager,
            LockManager lockManager,
            ExecutorService scanRequestExecutor,
            ClockService clockService,
            CatalogService catalogService,
            PartitionModificationCounterFactory partitionModificationCounterFactory,
            OutgoingSnapshotsManager outgoingSnapshotsManager,
            LowWatermark lowWatermark,
            ValidationSchemasSource validationSchemasSource,
            SchemaSyncService schemaSyncService,
            LeasePlacementDriver placementDriver,
            TopologyService topologyService,
            InternalClusterNode localNode,
            RemotelyTriggeredResourceRegistry remotelyTriggeredResourceRegistry,
            FailureProcessor failureProcessor,
            SchemaManager schemaManager,
            ReplicationConfiguration replicationConfiguration,
            Executor partitionOperationsExecutor,
            IndexMetaStorage indexMetaStorage,
            MinimumRequiredTimeCollectorService minTimeCollectorService,
            MvGc mvGc,
            FullStateTransferIndexChooser fullStateTransferIndexChooser
    ) {
        this.txManager = txManager;
        this.lockManager = lockManager;
        this.scanRequestExecutor = scanRequestExecutor;
        this.clockService = clockService;
        this.catalogService = catalogService;
        this.partitionModificationCounterFactory = partitionModificationCounterFactory;
        this.outgoingSnapshotsManager = outgoingSnapshotsManager;
        this.lowWatermark = lowWatermark;
        this.validationSchemasSource = validationSchemasSource;
        this.schemaSyncService = schemaSyncService;
        this.placementDriver = placementDriver;
        this.topologyService = topologyService;
        this.localNode = localNode;
        this.remotelyTriggeredResourceRegistry = remotelyTriggeredResourceRegistry;
        this.failureProcessor = failureProcessor;
        this.schemaManager = schemaManager;
        this.replicationConfiguration = replicationConfiguration;
        this.partitionOperationsExecutor = partitionOperationsExecutor;
        this.indexMetaStorage = indexMetaStorage;
        this.minTimeCollectorService = minTimeCollectorService;
        this.mvGc = mvGc;
        this.fullStateTransferIndexChooser = fullStateTransferIndexChooser;
    }

    /**
     * Creates a {@link PartitionDataStorage} for the given partition.
     *
     * @param partitionKey Partition key.
     * @param tableId Table ID.
     * @param partitionStorage MV partition storage.
     * @return Partition data storage.
     */
    PartitionDataStorage createPartitionDataStorage(PartitionKey partitionKey, int tableId, MvPartitionStorage partitionStorage) {
        return new SnapshotAwarePartitionDataStorage(
                tableId,
                partitionStorage,
                outgoingSnapshotsManager,
                partitionKey
        );
    }

    /**
     * Creates partition resources (index update handler, GC update handler, storage update handler, modification counter).
     *
     * <p>The returned resources are not started — the caller must invoke
     * {@link StorageUpdateHandler#start} on {@link PartitionResources#storageUpdateHandler}
     * before the constructed partition objects are used at runtime.
     *
     * @param partitionId Partition ID.
     * @param partitionDataStorage Partition data storage.
     * @param table Table view.
     * @param safeTimeTracker Safe time tracker.
     * @return Partition resources.
     */
    PartitionResources createPartitionResources(
            int partitionId,
            PartitionDataStorage partitionDataStorage,
            TableViewInternal table,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker
    ) {
        TableIndexStoragesSupplier indexes = table.indexStorageAdapters(partitionId);

        IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(indexes);

        GcUpdateHandler gcUpdateHandler = new GcUpdateHandler(partitionDataStorage, safeTimeTracker, indexUpdateHandler);

        SizeSupplier partSizeSupplier = () -> partitionDataStorage.getStorage().estimatedSize();

        PartitionModificationCounter modificationCounter =
                partitionModificationCounterFactory.create(partSizeSupplier, table::stalenessConfiguration, table.tableId(), partitionId);

        StorageUpdateHandler storageUpdateHandler = new StorageUpdateHandler(
                partitionId,
                partitionDataStorage,
                indexUpdateHandler,
                replicationConfiguration,
                modificationCounter,
                txManager
        );

        return new PartitionResources(storageUpdateHandler, indexUpdateHandler, gcUpdateHandler, modificationCounter);
    }

    /**
     * Creates a {@link TablePartitionProcessor} for the given partition.
     *
     * @param zonePartitionId Zone partition ID.
     * @param table Table view.
     * @param partitionDataStorage Partition data storage.
     * @param partitionResources Partition resources.
     * @return Table partition processor.
     */
    TablePartitionProcessor createTablePartitionProcessor(
            ZonePartitionId zonePartitionId,
            TableViewInternal table,
            PartitionDataStorage partitionDataStorage,
            PartitionResources partitionResources
    ) {
        return new TablePartitionProcessor(
                txManager,
                partitionDataStorage,
                partitionResources.storageUpdateHandler,
                catalogService,
                table.schemaView(),
                indexMetaStorage,
                localNode.id(),
                minTimeCollectorService,
                placementDriver,
                clockService,
                zonePartitionId
        );
    }

    /**
     * Creates a {@link PartitionMvStorageAccess} for the given partition.
     *
     * @param partitionId Partition ID.
     * @param table Table view.
     * @param partitionResources Partition resources.
     * @return Partition MV storage access.
     */
    PartitionMvStorageAccess createPartitionMvStorageAccess(
            int partitionId,
            TableViewInternal table,
            PartitionResources partitionResources
    ) {
        return new PartitionMvStorageAccessImpl(
                partitionId,
                table.internalTable().storage(),
                mvGc,
                partitionResources.indexUpdateHandler,
                partitionResources.gcUpdateHandler,
                fullStateTransferIndexChooser,
                schemaManager.schemaRegistry(table.tableId()),
                lowWatermark
        );
    }

    /**
     * Creates a {@link PartitionReplicaListener} for the given partition.
     *
     * @param replicationGroupId Zone partition ID used as the replication group ID.
     * @param table Table view.
     * @param safeTimeTracker Safe time tracker.
     * @param mvPartitionStorage MV partition storage.
     * @param partitionResources Partition resources.
     * @param raftClient Raft command runner.
     * @param transactionStateResolver Transaction state resolver.
     * @return Partition replica listener.
     */
    PartitionReplicaListener createReplicaListener(
            ZonePartitionId replicationGroupId,
            TableViewInternal table,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker,
            MvPartitionStorage mvPartitionStorage,
            PartitionResources partitionResources,
            RaftCommandRunner raftClient,
            TransactionStateResolver transactionStateResolver
    ) {
        int partitionIndex = replicationGroupId.partitionId();

        return new PartitionReplicaListener(
                mvPartitionStorage,
                new ExecutorInclinedRaftCommandRunner(raftClient, partitionOperationsExecutor),
                txManager,
                lockManager,
                scanRequestExecutor,
                replicationGroupId,
                table.tableId(),
                table.indexesLockers(partitionIndex),
                new Lazy<>(() -> table.indexStorageAdapters(partitionIndex).get().get(table.pkId())),
                () -> table.indexStorageAdapters(partitionIndex).get(),
                clockService,
                safeTimeTracker,
                transactionStateResolver,
                partitionResources.storageUpdateHandler,
                validationSchemasSource,
                localNode,
                schemaSyncService,
                catalogService,
                placementDriver,
                topologyService,
                remotelyTriggeredResourceRegistry,
                schemaManager.schemaRegistry(table.tableId()),
                indexMetaStorage,
                lowWatermark,
                failureProcessor,
                table.metrics()
        );
    }
}
