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

package org.apache.ignite.internal.partition.replicator.network.raft;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.jetbrains.annotations.Nullable;

/** Partition Raft snapshot meta. */
@Transferable(PartitionReplicationMessageGroup.PARTITION_SNAPSHOT_META)
public interface PartitionSnapshotMeta extends SnapshotMeta {
    /** Minimum catalog version that is required for the snapshot to be accepted by a follower. */
    int requiredCatalogVersion();

    /** Row ID for which the index needs to be built per building index ID at the time the snapshot meta was created. */
    @Nullable Map<Integer, UUID> nextRowIdToBuildByIndexId();

    /** Lease start time represented as {@link HybridTimestamp#longValue()}. */
    long leaseStartTime();

    /** ID of primary replica node ({@code null} if there is no primary). */
    @Nullable UUID primaryReplicaNodeId();

    /** Name of primary replica node ({@code null} if there is no primary). */
    @Nullable String primaryReplicaNodeName();
}
