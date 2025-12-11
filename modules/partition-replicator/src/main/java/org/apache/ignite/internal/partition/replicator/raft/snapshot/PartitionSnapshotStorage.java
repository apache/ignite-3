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

package org.apache.ignite.internal.partition.replicator.raft.snapshot;

import static it.unimi.dsi.fastutil.ints.Int2ObjectMaps.synchronize;
import static org.apache.ignite.internal.util.CompletableFutures.allOf;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.incoming.IncomingSnapshotCopier;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotMetricsSource;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotReader;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotCopier;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot storage for a zone partition.
 *
 * <p>Manages all table storages of a zone partition.
 *
 * <p>Utilizes the fact that every partition already stores its latest applied index and thus can itself be used as its own snapshot.
 *
 * <p>Uses {@link MvPartitionStorage#lastAppliedIndex()} and configuration to create a {@link SnapshotMeta} object
 * in {@link SnapshotReader#load()}.
 *
 * <p>Snapshot writer doesn't allow explicit save of any actual file. {@link SnapshotWriter#saveMeta(SnapshotMeta)} simply returns
 * {@code true}, and {@link SnapshotWriter#addFile(String)} throws an exception.
 */
public class PartitionSnapshotStorage {
    private static final IgniteLogger LOG = Loggers.forClass(PartitionSnapshotStorage.class);

    /** Default number of milliseconds that the follower is allowed to try to catch up the required catalog version. */
    private static final int DEFAULT_WAIT_FOR_METADATA_CATCHUP_MS = 3000;

    private final ZonePartitionKey partitionKey;

    private final TopologyService topologyService;

    /** Snapshot manager. */
    private final OutgoingSnapshotsManager outgoingSnapshotsManager;

    /**
     * Partition storages grouped by table ID.
     */
    private final Int2ObjectMap<PartitionMvStorageAccess> partitionsByTableId = synchronize(new Int2ObjectOpenHashMap<>());

    /**
     * Map with ongoing snapshot operations (either incoming or outgoing).
     *
     * <p>Concurrent access is guarded by {@link #snapshotOperationLock}.
     */
    private final Map<UUID, CompletableFuture<Void>> ongoingSnapshotOperations = new HashMap<>();

    private final Object snapshotOperationLock = new Object();

    private final PartitionTxStateAccess txState;

    private final CatalogService catalogService;

    private final FailureProcessor failureProcessor;

    /** Incoming snapshots executor. */
    private final Executor incomingSnapshotsExecutor;

    private final long waitForMetadataCatchupMs;

    private final LogStorageAccess logStorage;

    private final MetricManager metricManager;

    /** Constructor. */
    public PartitionSnapshotStorage(
            ZonePartitionKey partitionKey,
            TopologyService topologyService,
            OutgoingSnapshotsManager outgoingSnapshotsManager,
            PartitionTxStateAccess txState,
            CatalogService catalogService,
            FailureProcessor failureProcessor,
            Executor incomingSnapshotsExecutor,
            LogStorageAccess logStorage,
            MetricManager metricManager
    ) {
        this(
                partitionKey,
                topologyService,
                outgoingSnapshotsManager,
                txState,
                catalogService,
                failureProcessor,
                incomingSnapshotsExecutor,
                DEFAULT_WAIT_FOR_METADATA_CATCHUP_MS,
                logStorage, metricManager
        );
    }

    /** Constructor. */
    public PartitionSnapshotStorage(
            ZonePartitionKey partitionKey,
            TopologyService topologyService,
            OutgoingSnapshotsManager outgoingSnapshotsManager,
            PartitionTxStateAccess txState,
            CatalogService catalogService,
            FailureProcessor failureProcessor,
            Executor incomingSnapshotsExecutor,
            long waitForMetadataCatchupMs,
            LogStorageAccess logStorage,
            MetricManager metricManager
    ) {
        this.partitionKey = partitionKey;
        this.topologyService = topologyService;
        this.outgoingSnapshotsManager = outgoingSnapshotsManager;
        this.txState = txState;
        this.catalogService = catalogService;
        this.failureProcessor = failureProcessor;
        this.incomingSnapshotsExecutor = incomingSnapshotsExecutor;
        this.waitForMetadataCatchupMs = waitForMetadataCatchupMs;
        this.logStorage = logStorage;
        this.metricManager = metricManager;
    }

    public PartitionKey partitionKey() {
        return partitionKey;
    }

    public TopologyService topologyService() {
        return topologyService;
    }

    public MessagingService messagingService() {
        return outgoingSnapshotsManager.messagingService();
    }

    public OutgoingSnapshotsManager outgoingSnapshotsManager() {
        return outgoingSnapshotsManager;
    }

    /**
     * Returns partitions by table ID.
     */
    public Int2ObjectMap<PartitionMvStorageAccess> partitionsByTableId() {
        synchronized (partitionsByTableId) {
            return new Int2ObjectOpenHashMap<>(partitionsByTableId);
        }
    }

    /**
     * Adds a given table storage to the set of managed storages.
     */
    public void addMvPartition(int tableId, PartitionMvStorageAccess partition) {
        PartitionMvStorageAccess prev = partitionsByTableId.put(tableId, partition);

        assert prev == null : partitionKey + ": partition storage for table ID " + tableId + " already exists.";
    }

    /**
     * Removes a given table storage from the set of managed storages.
     *
     * <p>If there exists an ongoing incoming or outgoing snapshot, the deletion will be deferred until the snapshot is completed.
     */
    public CompletableFuture<Void> removeMvPartition(int tableId) {
        synchronized (snapshotOperationLock) {
            if (ongoingSnapshotOperations.isEmpty()) {
                partitionsByTableId.remove(tableId);

                return nullCompletedFuture();
            } else {
                return allOf(ongoingSnapshotOperations.values()).thenCompose(v -> removeMvPartition(tableId));
            }
        }
    }

    /**
     * Returns the TX state storage.
     */
    public PartitionTxStateAccess txState() {
        return txState;
    }

    /**
     * Returns catalog service.
     */
    public CatalogService catalogService() {
        return catalogService;
    }

    /**
     * Returns failure processor.
     */
    public FailureProcessor failureProcessor() {
        return failureProcessor;
    }

    /**
     * Starts an incoming snapshot.
     */
    public SnapshotCopier startIncomingSnapshot(String uri) {
        UUID snapshotId = UUID.randomUUID();

        LOG.info("Starting incoming snapshot [partitionKey={}, uri={}, snapshotId={}]", partitionKey, uri, snapshotId);

        startSnapshotOperation(snapshotId);

        SnapshotUri snapshotUri = SnapshotUri.fromStringUri(uri);

        var copier = new IncomingSnapshotCopier(this, snapshotUri, incomingSnapshotsExecutor, waitForMetadataCatchupMs) {
            @Override
            public void close() {
                try {
                    super.close();
                } finally {
                    completeSnapshotOperation(snapshotId);
                }
            }
        };

        copier.start();

        return copier;
    }

    /**
     * Starts an outgoing snapshot.
     */
    public SnapshotReader startOutgoingSnapshot() {
        UUID snapshotId = UUID.randomUUID();

        LOG.info("Starting outgoing snapshot [partitionKey={}, snapshotId={}]", partitionKey, snapshotId);

        startSnapshotOperation(snapshotId);

        var metricSource = new OutgoingSnapshotMetricsSource(snapshotId, partitionKey);

        metricManager.registerSource(metricSource);
        metricManager.enable(metricSource);

        return new OutgoingSnapshotReader(snapshotId, this, metricSource) {
            @Override
            public void close() throws IOException {
                try {
                    super.close();
                } finally {
                    completeSnapshotOperation(snapshotId);
                }
            }
        };
    }

    private void startSnapshotOperation(UUID snapshotId) {
        synchronized (snapshotOperationLock) {
            CompletableFuture<Void> previousFuture = ongoingSnapshotOperations.put(snapshotId, new CompletableFuture<>());

            assert previousFuture == null :
                    String.format("Snapshot already in progress [partitionId=%s, snapshotId=%s]", partitionKey, snapshotId);
        }
    }

    private void completeSnapshotOperation(UUID snapshotId) {
        synchronized (snapshotOperationLock) {
            LOG.info("Finishing outgoing snapshot [partitionKey={}, snapshotId={}]", partitionKey, snapshotId);

            CompletableFuture<Void> operationFuture = ongoingSnapshotOperations.remove(snapshotId);

            assert operationFuture != null :
                    String.format("No snapshot in progress [partitionId=%s, snapshotId=%s]", partitionKey, snapshotId);

            operationFuture.complete(null);
        }
    }

    /**
     * Computes a startup snapshot meta based on the current storage states or returns {@code null} if the storages are empty.
     */
    public @Nullable SnapshotMeta readStartupSnapshotMeta() {
        // We must choose the minimum applied index for local recovery so that we don't skip the raft commands for the storage with the
        // lowest applied index and thus no data loss occurs.
        PartitionMvStorageAccess storageWithMinLastAppliedIndex = null;

        long minLastAppliedIndex = Long.MAX_VALUE;

        for (PartitionMvStorageAccess partitionStorage : partitionsByTableId.values()) {
            long lastAppliedIndex = partitionStorage.lastAppliedIndex();

            if (lastAppliedIndex < 0) {
                throw new IllegalStateException(String.format(
                        "MV partition storage [tableId=%d, zoneId=%d, partitionId=%d] contains an unexpected applied index value: %d.",
                        partitionStorage.tableId(),
                        partitionKey.zoneId(),
                        partitionStorage.partitionId(),
                        lastAppliedIndex
                ));
            }

            if (lastAppliedIndex == 0) {
                return null;
            }

            if (lastAppliedIndex < minLastAppliedIndex) {
                minLastAppliedIndex = lastAppliedIndex;
                storageWithMinLastAppliedIndex = partitionStorage;
            }
        }

        long txStateLastAppliedIndex = txState.lastAppliedIndex();
        if (txStateLastAppliedIndex < 0) {
            throw new IllegalStateException(
                    String.format("Tx state partition storage [key=%s] contains an unexpected applied index value: %d.",
                            partitionKey,
                            txStateLastAppliedIndex
                    )
            );
        }

        if (txStateLastAppliedIndex < minLastAppliedIndex) {
            return startupSnapshotMetaFromTxStorage();
        } else {
            assert storageWithMinLastAppliedIndex != null;

            return startupSnapshotMetaFromPartitionStorage(storageWithMinLastAppliedIndex);
        }
    }

    private @Nullable SnapshotMeta startupSnapshotMetaFromTxStorage() {
        long lastAppliedIndex = txState.lastAppliedIndex();

        if (lastAppliedIndex == 0) {
            return null;
        }

        RaftGroupConfiguration configuration = txState.committedGroupConfiguration();

        assert configuration != null : partitionKey + ": empty configuration in startup snapshot.";

        return startupSnapshotMeta(lastAppliedIndex, txState.lastAppliedTerm(), configuration);
    }

    private SnapshotMeta startupSnapshotMetaFromPartitionStorage(PartitionMvStorageAccess partitionStorage) {
        RaftGroupConfiguration configuration = partitionStorage.committedGroupConfiguration();

        assert configuration != null : partitionKey + ": empty configuration in startup snapshot.";

        return startupSnapshotMeta(partitionStorage.lastAppliedIndex(), partitionStorage.lastAppliedTerm(), configuration);
    }

    private static SnapshotMeta startupSnapshotMeta(long lastAppliedIndex, long lastAppliedTerm, RaftGroupConfiguration configuration) {
        return new RaftMessagesFactory().snapshotMeta()
                .lastIncludedIndex(lastAppliedIndex)
                .lastIncludedTerm(lastAppliedTerm)
                .cfgIndex(configuration.index())
                .cfgTerm(configuration.term())
                .peersList(configuration.peers())
                .oldPeersList(configuration.oldPeers())
                .learnersList(configuration.learners())
                .oldLearnersList(configuration.oldLearners())
                .build();
    }

    /** Returns the replication log storage. */
    public LogStorageAccess logStorage() {
        return logStorage;
    }
}
