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

package org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing;

import java.util.UUID;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionKey;

/**
 * Allows to obtain {@link PartitionSnapshots} instances.
 */
public interface PartitionsSnapshots {
    /**
     * Returns {@link PartitionSnapshots} corresponding to the given partition.
     *
     * @param partitionKey Partition key.
     * @return PartitionSnapshots instance.
     */
    PartitionSnapshots partitionSnapshots(PartitionKey partitionKey);

    /**
     * Removes the underlying collection for snapshots of this partition.
     *
     * @param partitionKey Partition key.
     */
    void removeSnapshots(PartitionKey partitionKey);

    /**
     * Finishes a snapshot. This closes the snapshot and deregisters it.
     *
     * @param snapshotId ID of the snapshot to finish.
     */
    void finishOutgoingSnapshot(UUID snapshotId);
}
