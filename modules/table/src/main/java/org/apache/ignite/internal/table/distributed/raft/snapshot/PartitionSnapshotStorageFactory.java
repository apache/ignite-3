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

package org.apache.ignite.internal.table.distributed.raft.snapshot;

import java.util.List;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.raft.storage.SnapshotStorageFactory;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.SnapshotStorage;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotWriter;

/**
 * Snapshot storage factory for {@link MvPartitionStorage}. Utilizes the fact that every partition already stores its latest applied index
 * and thus can itself be used as its own snapshot.
 *
 * <p/>Uses {@link MvPartitionStorage#persistedIndex()} and configuration, passed into constructor, to create a {@link SnapshotMeta} object
 * in {@link SnapshotReader#load()}.
 *
 * <p/>Snapshot writer doesn't allow explicit save of any actual file. {@link SnapshotWriter#saveMeta(SnapshotMeta)} simply returns
 * {@code true}, and {@link SnapshotWriter#addFile(String)} throws an exception.
 */
public class PartitionSnapshotStorageFactory implements SnapshotStorageFactory {
    /** Topology service. */
    private final TopologyService topologyService;

    /** Snapshot manager. */
    private final OutgoingSnapshotsManager outgoingSnapshotsManager;

    /** Partition storage. */
    private final PartitionAccess partition;

    /** List of peers. */
    private final List<String> peers;

    /** List of learners. */
    private final List<String> learners;

    /** RAFT log index. */
    private final long persistedRaftIndex;

    /** Incoming snapshots executor. */
    private final Executor incomingSnapshotsExecutor;

    /**
     * Constructor.
     *
     * @param topologyService Topology service.
     * @param outgoingSnapshotsManager Snapshot manager.
     * @param partition MV partition storage.
     * @param peers List of raft group peers to be used in snapshot meta.
     * @param learners List of raft group learners to be used in snapshot meta.
     * @param incomingSnapshotsExecutor Incoming snapshots executor.
     * @see SnapshotMeta
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public PartitionSnapshotStorageFactory(
            TopologyService topologyService,
            OutgoingSnapshotsManager outgoingSnapshotsManager,
            PartitionAccess partition,
            List<String> peers,
            List<String> learners,
            Executor incomingSnapshotsExecutor
    ) {
        this.topologyService = topologyService;
        this.outgoingSnapshotsManager = outgoingSnapshotsManager;
        this.partition = partition;
        this.peers = peers;
        this.learners = learners;
        this.incomingSnapshotsExecutor = incomingSnapshotsExecutor;

        // We must choose the minimum applied index for local recovery so that we don't skip the raft commands for the storage with the
        // lowest applied index and thus no data loss occurs.
        persistedRaftIndex = Math.min(
                partition.mvPartitionStorage().persistedIndex(),
                partition.txStatePartitionStorage().persistedIndex()
        );
    }

    @Override
    public SnapshotStorage createSnapshotStorage(String uri, RaftOptions raftOptions) {
        SnapshotMeta snapshotMeta = new RaftMessagesFactory().snapshotMeta()
                .lastIncludedIndex(persistedRaftIndex)
                // According to the code of org.apache.ignite.raft.jraft.core.NodeImpl.bootstrap, it's "dangerous" to init term with a value
                // greater than 1. 0 value of persisted index means that the underlying storage is empty.
                .lastIncludedTerm(persistedRaftIndex > 0 ? 1 : 0)
                .peersList(peers)
                .learnersList(learners)
                .build();

        return new PartitionSnapshotStorage(
                topologyService,
                outgoingSnapshotsManager,
                uri,
                raftOptions,
                partition,
                snapshotMeta,
                incomingSnapshotsExecutor
        );
    }
}
