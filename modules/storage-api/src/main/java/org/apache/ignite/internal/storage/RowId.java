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

package org.apache.ignite.internal.storage;

import java.util.UUID;
import org.apache.ignite.internal.tx.Timestamp;

/**
 * Class that represents row id in primary index of the table.
 *
 * @see MvPartitionStorage
 */
public final class RowId {
    private final short partitionId;

    private final UUID uuid;

    public RowId(int partitionId, Timestamp timestamp) {
        this(partitionId, timestamp.toUuid());
    }

    public RowId(int partitionId, long mostSignificantBits, long leastSignificantBits) {
        this(partitionId, new UUID(mostSignificantBits, leastSignificantBits));
    }

    private RowId(int partitionId, UUID uuid) {
        this.partitionId = (short) (partitionId & 0xFFFF);
        this.uuid = uuid;
    }

    /**
     * Returns a partition id for current row id.
     */
    public int partitionId() {
        return partitionId & 0xFFFF;
    }

    public long mostSignificantBits() {
        return uuid.getMostSignificantBits();
    }

    public long leastSignificantBits() {
        return uuid.getLeastSignificantBits();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RowId rowId = (RowId) o;

        if (partitionId != rowId.partitionId) {
            return false;
        }
        return uuid.equals(rowId.uuid);
    }

    @Override
    public int hashCode() {
        int result = partitionId;
        result = 31 * result + uuid.hashCode();
        return result;
    }
}
