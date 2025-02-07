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

import static org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.SnapshotMetaUtils.collectNextRowIdToBuildIndexes;
import static org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.SnapshotMetaUtils.snapshotMetaAt;

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
    /** Topology service. */
    private final TopologyService topologyService;

    /** Snapshot manager. */
    private final OutgoingSnapshotsManager outgoingSnapshotsManager;

    /** Partition storage. */
    private final PartitionAccess partition;

    private final CatalogService catalogService;

    private final @Nullable SnapshotMeta startupSnapshotMeta;

    /** Incoming snapshots executor. */
    private final Executor incomingSnapshotsExecutor;

    /**
     * Constructor.
     *
     * @param topologyService Topology service.
     * @param outgoingSnapshotsManager Snapshot manager.
     * @param partition MV partition storage.
     * @param catalogService Access to the Catalog.
     * @param incomingSnapshotsExecutor Incoming snapshots executor.
     * @see SnapshotMeta
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public PartitionSnapshotStorageFactory(
            TopologyService topologyService,
            OutgoingSnapshotsManager outgoingSnapshotsManager,
            PartitionAccess partition,
            CatalogService catalogService,
            Executor incomingSnapshotsExecutor
    ) {
        this.topologyService = topologyService;
        this.outgoingSnapshotsManager = outgoingSnapshotsManager;
        this.partition = partition;
        this.catalogService = catalogService;
        this.incomingSnapshotsExecutor = incomingSnapshotsExecutor;

        // We must choose the minimum applied index for local recovery so that we don't skip the raft commands for the storage with the
        // lowest applied index and thus no data loss occurs.
        long lastIncludedRaftIndex = partition.minLastAppliedIndex();
        long lastIncludedRaftTerm = partition.minLastAppliedTerm();

        int lastCatalogVersionAtStart = catalogService.latestCatalogVersion();

        if (lastIncludedRaftIndex == 0) {
            startupSnapshotMeta = null;
        } else {
            startupSnapshotMeta = snapshotMetaAt(
                    lastIncludedRaftIndex,
                    lastIncludedRaftTerm,
                    Objects.requireNonNull(partition.committedGroupConfiguration()),
                    lastCatalogVersionAtStart,
                    collectNextRowIdToBuildIndexesAtStart(lastCatalogVersionAtStart),
                    partition.leaseStartTime(),
                    partition.primaryReplicaNodeId(),
                    partition.primaryReplicaNodeName()
            );
        }
    }

    @Override
    public @Nullable PartitionSnapshotStorage createSnapshotStorage(String uri, RaftOptions raftOptions) {
        return new PartitionSnapshotStorage(
                topologyService,
                outgoingSnapshotsManager,
                uri,
                raftOptions,
                partition,
                catalogService,
                startupSnapshotMeta,
                incomingSnapshotsExecutor
        );
    }

    private Map<Integer, UUID> collectNextRowIdToBuildIndexesAtStart(int lastCatalogVersionAtStart) {
        return collectNextRowIdToBuildIndexes(catalogService, partition, lastCatalogVersionAtStart);
    }
}
