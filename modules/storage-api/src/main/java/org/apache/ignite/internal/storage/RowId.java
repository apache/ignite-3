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

package org.apache.ignite.internal.storage;

import java.io.Serializable;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Class that represents row ID in primary index of the table. Contains a timestamp-based UUID and a partition ID.
 *
 * @see MvPartitionStorage
 */
public final class RowId implements Serializable, Comparable<RowId> {
    /** Partition ID. Short type reduces payload when transferring an object over network. */
    private final short partitionId;

    /** Unique ID. */
    private final UUID uuid;

    public static RowId lowestRowId(int partitionId) {
        return new RowId(partitionId, Long.MIN_VALUE, Long.MIN_VALUE);
    }

    public static RowId highestRowId(int partitionId) {
        return new RowId(partitionId, Long.MAX_VALUE, Long.MAX_VALUE);
    }

    /**
     * Create a row ID with the UUID value based on {@link UUID#randomUUID()}.
     * Intended for tests only, because random UUIDs are very slow when it comes to frequent usages.
     *
     * @param partitionId Partition ID.
     */
    @TestOnly
    public RowId(int partitionId) {
        this(partitionId, UUID.randomUUID());
    }

    /**
     * Constructor.
     *
     * @param partitionId Partition ID.
     * @param mostSignificantBits UUID's most significant bits.
     * @param leastSignificantBits UUID's least significant bits.
     */
    public RowId(int partitionId, long mostSignificantBits, long leastSignificantBits) {
        this(partitionId, new UUID(mostSignificantBits, leastSignificantBits));
    }

    /**
     * Constructor.
     *
     * @param partitionId Partition ID.
     * @param uuid UUID.
     */
    public RowId(int partitionId, UUID uuid) {
        this.partitionId = (short) partitionId;
        this.uuid = uuid;
    }

    /** Returns a partition ID for current row ID. */
    public int partitionId() {
        return partitionId & 0xFFFF;
    }

    /** Returns the most significant 64 bits of row ID's UUID.*/
    public long mostSignificantBits() {
        return uuid.getMostSignificantBits();
    }

    /** Returns the least significant 64 bits of row ID's UUID. */
    public long leastSignificantBits() {
        return uuid.getLeastSignificantBits();
    }

    /** Returns the UUID equivalent of {@link #mostSignificantBits()} and {@link #leastSignificantBits()}. */
    public UUID uuid() {
        return uuid;
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

        return partitionId == rowId.partitionId && uuid.equals(rowId.uuid);

    }

    @Override
    public int hashCode() {
        int result = partitionId;
        result = 31 * result + uuid.hashCode();
        return result;
    }

    @Override
    public int compareTo(RowId rowId) {
        int cmp = Short.compareUnsigned(partitionId, rowId.partitionId);

        if (cmp != 0) {
            return cmp;
        }

        return uuid.compareTo(rowId.uuid);
    }

    /** Returns the next row ID withing a single partition, or {@code null} if current row ID already has maximal possible value. */
    public @Nullable RowId increment() {
        long lsb = uuid.getLeastSignificantBits() + 1;

        long msb = uuid.getMostSignificantBits();

        if (lsb == Long.MIN_VALUE) {
            ++msb;

            if (msb == Long.MIN_VALUE) {
                return null;
            }
        }

        return new RowId(partitionId, msb, lsb);
    }

    @Override
    public String toString() {
        return "RowId [partitionId=" + partitionId() + ", uuid=" + uuid + ']';
    }
}
