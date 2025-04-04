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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.storage.SnapshotStorageFactory;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
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

    private final FailureProcessor failureProcessor;

    /** Incoming snapshots executor. */
    private final Executor incomingSnapshotsExecutor;

    /** Constructor. */
    public PartitionSnapshotStorageFactory(
            PartitionKey partitionKey,
            TopologyService topologyService,
            OutgoingSnapshotsManager outgoingSnapshotsManager,
            PartitionTxStateAccess txStateStorage,
            CatalogService catalogService,
            FailureProcessor failureProcessor,
            Executor incomingSnapshotsExecutor
    ) {
        this.partitionKey = partitionKey;
        this.topologyService = topologyService;
        this.outgoingSnapshotsManager = outgoingSnapshotsManager;
        this.txStateStorage = txStateStorage;
        this.catalogService = catalogService;
        this.failureProcessor = failureProcessor;
        this.incomingSnapshotsExecutor = incomingSnapshotsExecutor;
    }

    /**
     * Adds a given table partition storage to the snapshot storage, managed by this factory.
     */
    public void addMvPartition(int tableId, PartitionMvStorageAccess partition) {
        PartitionMvStorageAccess prev = partitionsByTableId.put(tableId, partition);

        assert prev == null : "Partition storage for table ID " + tableId + " already exists.";
    }

    public void removeMvPartition(int tableId) {
        partitionsByTableId.remove(tableId);
    }

    @Override
    public PartitionSnapshotStorage createSnapshotStorage(String uri, RaftOptions raftOptions) {
        return new PartitionSnapshotStorage(
                partitionKey,
                topologyService,
                outgoingSnapshotsManager,
                uri,
                raftOptions,
                partitionsByTableId,
                txStateStorage,
                catalogService,
                failureProcessor,
                createStartupSnapshotMeta(),
                incomingSnapshotsExecutor
        );
    }

    private @Nullable SnapshotMeta createStartupSnapshotMeta() {
        // We must choose the minimum applied index for local recovery so that we don't skip the raft commands for the storage with the
        // lowest applied index and thus no data loss occurs.
        PartitionMvStorageAccess storageWithMinLastAppliedIndex = null;

        long minLastAppliedIndex = Long.MAX_VALUE;

        for (PartitionMvStorageAccess partitionStorage : partitionsByTableId.values()) {
            long lastAppliedIndex = partitionStorage.lastAppliedIndex();

            assert lastAppliedIndex >= 0 :
                    String.format("Partition storage [tableId=%d, partitionId=%d] contains an unexpected applied index value: %d.",
                            partitionStorage.tableId(),
                            partitionStorage.partitionId(),
                            lastAppliedIndex
                    );

            if (lastAppliedIndex == 0) {
                return null;
            }

            if (lastAppliedIndex < minLastAppliedIndex) {
                minLastAppliedIndex = lastAppliedIndex;
                storageWithMinLastAppliedIndex = partitionStorage;
            }
        }

        if (txStateStorage.lastAppliedIndex() < minLastAppliedIndex) {
            return startupSnapshotMetaFromTxStorage();
        } else {
            assert storageWithMinLastAppliedIndex != null;

            return startupSnapshotMetaFromPartitionStorage(storageWithMinLastAppliedIndex);
        }
    }

    private @Nullable SnapshotMeta startupSnapshotMetaFromTxStorage() {
        long lastAppliedIndex = txStateStorage.lastAppliedIndex();

        if (lastAppliedIndex == 0) {
            return null;
        }

        RaftGroupConfiguration configuration = txStateStorage.committedGroupConfiguration();

        assert configuration != null : "Empty configuration in startup snapshot.";

        return startupSnapshotMeta(lastAppliedIndex, txStateStorage.lastAppliedTerm(), configuration);
    }

    private static SnapshotMeta startupSnapshotMetaFromPartitionStorage(PartitionMvStorageAccess partitionStorage) {
        RaftGroupConfiguration configuration = partitionStorage.committedGroupConfiguration();

        assert configuration != null : "Empty configuration in startup snapshot.";

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
}
