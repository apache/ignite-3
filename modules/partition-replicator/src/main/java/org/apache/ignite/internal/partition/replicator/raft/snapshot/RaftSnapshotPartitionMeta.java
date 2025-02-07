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

import java.util.UUID;
import org.apache.ignite.internal.partition.replicator.network.raft.PartitionSnapshotMeta;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.storage.engine.MvPartitionMeta;
import org.apache.ignite.internal.storage.engine.PrimitivePartitionMeta;
import org.jetbrains.annotations.Nullable;

/**
 * Partition metadata for {@link PartitionAccess}.
 */
public class RaftSnapshotPartitionMeta extends PrimitivePartitionMeta {
    private final RaftGroupConfiguration raftGroupConfig;

    /** Constructs an {@link RaftSnapshotPartitionMeta} from a {@link PartitionSnapshotMeta} . */
    public static RaftSnapshotPartitionMeta fromSnapshotMeta(PartitionSnapshotMeta meta, RaftGroupConfiguration raftGroupConfig) {
        return new RaftSnapshotPartitionMeta(
                meta.lastIncludedIndex(),
                meta.lastIncludedTerm(),
                raftGroupConfig,
                meta.leaseStartTime(),
                meta.primaryReplicaNodeId(),
                meta.primaryReplicaNodeName()
        );
    }

    /** Constructor. */
    private RaftSnapshotPartitionMeta(
            long lastAppliedIndex,
            long lastAppliedTerm,
            RaftGroupConfiguration raftGroupConfig,
            long leaseStartTime,
            @Nullable UUID primaryReplicaNodeId,
            @Nullable String primaryReplicaNodeName
    ) {
        super(lastAppliedIndex, lastAppliedTerm, leaseStartTime, primaryReplicaNodeId, primaryReplicaNodeName);

        this.raftGroupConfig = raftGroupConfig;
    }

    /** Returns replication group config. */
    public RaftGroupConfiguration raftGroupConfig() {
        return raftGroupConfig;
    }

    /**
     * Converts this meta to {@link MvPartitionMeta}.
     *
     * @param configBytes Group config represented as bytes.
     */
    public MvPartitionMeta toMvPartitionMeta(byte[] configBytes) {
        return new MvPartitionMeta(
                lastAppliedIndex(),
                lastAppliedTerm(),
                configBytes,
                leaseStartTime(),
                primaryReplicaNodeId(),
                primaryReplicaNodeName()
        );
    }
}
