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

import java.util.Objects;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tostring.S;

/**
 * Uniquely identifies a partition. This is a pair of internal table ID and partition number (aka partition ID).
 */
// TODO IGNITE-22522 Remove
public class TablePartitionKey implements PartitionKey {
    private final int tableId;

    private final int partitionId;

    /**
     * Returns ID of the table.
     */
    public int tableId() {
        return tableId;
    }

    /**
     * Returns partition ID.
     */
    @Override
    public int partitionId() {
        return partitionId;
    }

    /**
     * Constructs a new partition key.
     */
    public TablePartitionKey(int tableId, int partitionId) {
        this.tableId = tableId;
        this.partitionId = partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TablePartitionKey that = (TablePartitionKey) o;
        return partitionId == that.partitionId && tableId == that.tableId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, partitionId);
    }

    @Override
    public String toString() {
        return S.toString(TablePartitionKey.class, this);
    }

    @Override
    public ReplicationGroupId toReplicationGroupId() {
        return new TablePartitionId(tableId, partitionId);
    }
}
