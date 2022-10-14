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
import java.util.UUID;

/**
 * Uniquely identifies a partition. This is a pair of internal table ID and partition number (aka partition ID).
 */
public class PartitionKey {
    private final UUID tableId;
    private final int partitionId;

    /**
     * Returns partition ID.
     *
     * @return Partition ID.
     */
    public int partitionId() {
        return partitionId;
    }

    /**
     * Constructs a new partition key.
     */
    public PartitionKey(UUID tableId, int partitionId) {
        Objects.requireNonNull(tableId, "tableId cannot be null");

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
        PartitionKey that = (PartitionKey) o;
        return partitionId == that.partitionId && tableId.equals(that.tableId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, partitionId);
    }

    @Override
    public String toString() {
        return "PartitionKey{"
                + "tableId=" + tableId
                + ", partitionId=" + partitionId
                + '}';
    }
}
