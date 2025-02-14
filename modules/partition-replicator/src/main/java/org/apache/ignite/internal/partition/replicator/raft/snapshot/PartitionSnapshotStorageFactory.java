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
import static java.lang.Math.min;
import static java.util.Comparator.comparingLong;
import static org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.SnapshotMetaUtils.collectNextRowIdToBuildIndexes;
import static org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.SnapshotMetaUtils.snapshotMetaAt;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.raft.storage.SnapshotStorageFactory;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot storage factory for {@link MvPartitionStorage}. Utilizes the fact that every partition already stores its latest applied index
 * and thus can itself be used as its own snapshot.
 *
 * <p>Uses {@link MvPartitionStorage#lastAppliedIndex()} and configuration, passed into constructor, to create a {@link SnapshotMeta} object
 * in {@link SnapshotReader#load()}.
 *
 * <p>Snapshot writer doesn't allow explicit save of any actual file. {@link SnapshotWriter#saveMeta(SnapshotMeta)} simply returns
 * {@code true}, and {@link SnapshotWriter#addFile(String)} throws an exception.
 */
public class PartitionSnapshotStorageFactory implements SnapshotStorageFactory {
    private final PartitionKey partitionKey;

    /** Topology service. */
    private final TopologyService topologyService;

    /** Snapshot manager. */
    private final OutgoingSnapshotsManager outgoingSnapshotsManager;

    /**
     * Partition storages grouped by table ID.
     */
    private final Int2ObjectMap<PartitionMvStorageAccess> partitionsByTableId = synchronize(new Int2ObjectOpenHashMap<>());

    private final PartitionTxStateAccess txStateStorage;

    private final CatalogService catalogService;

    /** Incoming snapshots executor. */
    private final Executor incomingSnapshotsExecutor;

    /** Constructor. */
    public PartitionSnapshotStorageFactory(
            PartitionKey partitionKey,
            TopologyService topologyService,
            OutgoingSnapshotsManager outgoingSnapshotsManager,
            PartitionTxStateAccess txStateStorage,
            CatalogService catalogService,
            Executor incomingSnapshotsExecutor
    ) {
        this.partitionKey = partitionKey;
        this.topologyService = topologyService;
        this.outgoingSnapshotsManager = outgoingSnapshotsManager;
        this.txStateStorage = txStateStorage;
        this.catalogService = catalogService;
        this.incomingSnapshotsExecutor = incomingSnapshotsExecutor;
    }

    /**
     * Adds a given table partition storage to the snapshot storage, managed by this factory.
     */
    public void addMvPartition(int tableId, PartitionMvStorageAccess partition) {
        // FIXME: there are possible races with table creation, see https://issues.apache.org/jira/browse/IGNITE-24522
        PartitionMvStorageAccess prev = partitionsByTableId.put(tableId, partition);

        assert prev == null : "Partition storage for table ID " + tableId + " already exists.";
    }

    public void removeMvPartition(int tableId) {
        partitionsByTableId.remove(tableId);
    }

    @Override
    public @Nullable PartitionSnapshotStorage createSnapshotStorage(String uri, RaftOptions raftOptions) {
        return new PartitionSnapshotStorage(
                partitionKey,
                topologyService,
                outgoingSnapshotsManager,
                uri,
                raftOptions,
                partitionsByTableId,
                txStateStorage,
                catalogService,
                createStartupSnapshotMeta(),
                incomingSnapshotsExecutor
        );
    }

    private @Nullable SnapshotMeta createStartupSnapshotMeta() {
        // We must choose the minimum applied index for local recovery so that we don't skip the raft commands for the storage with the
        // lowest applied index and thus no data loss occurs.
        return partitionsByTableId.values().stream()
                .min(comparingLong(PartitionMvStorageAccess::lastAppliedIndex))
                .map(storageWithMinLastAppliedIndex -> {
                    long minLastAppliedIndex = min(storageWithMinLastAppliedIndex.lastAppliedIndex(), txStateStorage.lastAppliedIndex());

                    if (minLastAppliedIndex == 0) {
                        return null;
                    }

                    int lastCatalogVersionAtStart = catalogService.latestCatalogVersion();

                    return snapshotMetaAt(
                            minLastAppliedIndex,
                            min(storageWithMinLastAppliedIndex.lastAppliedTerm(), txStateStorage.lastAppliedTerm()),
                            Objects.requireNonNull(storageWithMinLastAppliedIndex.committedGroupConfiguration()),
                            lastCatalogVersionAtStart,
                            collectNextRowIdToBuildIndexesAtStart(lastCatalogVersionAtStart),
                            storageWithMinLastAppliedIndex.leaseStartTime(),
                            storageWithMinLastAppliedIndex.primaryReplicaNodeId(),
                            storageWithMinLastAppliedIndex.primaryReplicaNodeName()
                    );
                })
                .orElse(null);
    }

    private Map<Integer, UUID> collectNextRowIdToBuildIndexesAtStart(int lastCatalogVersionAtStart) {
        return collectNextRowIdToBuildIndexes(catalogService, partitionsByTableId.values(), lastCatalogVersionAtStart);
    }
}
