/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.ignite.internal.raft.storage.SnapshotStorageFactory;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.SnapshotStorage;

public class PartitionSnapshotStorageFactory implements SnapshotStorageFactory {
    private final MvPartitionStorage partitionStorage;
    private final List<String> peers;
    private final List<String> learners;
    private final long persistedRaftIndex;

    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public PartitionSnapshotStorageFactory(MvPartitionStorage partitionStorage, List<String> peers, List<String> learners) {
        this.partitionStorage = partitionStorage;
        this.peers = peers;
        this.learners = learners;

        persistedRaftIndex = partitionStorage.persistedIndex();
    }

    @Override
    public SnapshotStorage createSnapshotStorage(String uri, RaftOptions raftOptions) {
        SnapshotMeta snapshotMeta = new RaftMessagesFactory().snapshotMeta()
                .lastIncludedIndex(persistedRaftIndex)
                .lastIncludedTerm(persistedRaftIndex > 0 ? 1 : 0)
                .peersList(peers)
                .learnersList(learners)
                .build();

        return new PartitionSnapshotStorage(uri, raftOptions, partitionStorage, snapshotMeta);
    }
}
