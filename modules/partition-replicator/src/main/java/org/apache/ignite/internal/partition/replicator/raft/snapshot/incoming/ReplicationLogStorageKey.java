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

package org.apache.ignite.internal.partition.replicator.raft.snapshot.incoming;

import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionMvStorageAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;

/** Helper class for starting rebalancing for the replication log. */
class ReplicationLogStorageKey {
    @IgniteToStringInclude
    private final ReplicationGroupId replicationGroupId;

    private final boolean isVolatile;

    private ReplicationLogStorageKey(ReplicationGroupId replicationGroupId, boolean isVolatile) {
        this.replicationGroupId = replicationGroupId;
        this.isVolatile = isVolatile;
    }

    static ReplicationLogStorageKey create(PartitionSnapshotStorage snapshotStorage, PartitionMvStorageAccess mvStorage) {
        return new ReplicationLogStorageKey(snapshotStorage.partitionKey().toReplicationGroupId(), mvStorage.isVolatile());
    }

    ReplicationGroupId replicationGroupId() {
        return replicationGroupId;
    }

    boolean isVolatile() {
        return isVolatile;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ReplicationLogStorageKey that = (ReplicationLogStorageKey) o;

        return isVolatile == that.isVolatile && replicationGroupId.equals(that.replicationGroupId);
    }

    @Override
    public int hashCode() {
        int result = replicationGroupId.hashCode();
        result = 31 * result + (isVolatile ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return S.toString(ReplicationLogStorageKey.class, this);
    }
}
