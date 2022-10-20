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

package org.apache.ignite.internal.replicator.message;

import java.io.Serializable;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The class stores a table id together with a partition id.
 * It is used as a replication group identifier.
 */
public class ReplicationGroupId implements Comparable<ReplicationGroupId>, Serializable {
    /** Table id. */
    private final UUID tableId;

    /** Partition id. */
    private final int partId;

    /**
     * The constructor.
     *
     * @param tableId Table id.
     * @param partId Partition id.
     */
    public ReplicationGroupId(@Nullable UUID tableId, int partId) {
        this.tableId = tableId;
        this.partId = partId;
    }

    /**
     * Gets a pration id.
     *
     * @return Partition id.
     */
    public int getPartId() {
        return partId;
    }

    /**
     * Gets a table id.
     *
     * @return Table id.
     */
    public UUID getTableId() {
        return tableId;
    }

    @Override
    public int compareTo(@NotNull ReplicationGroupId o) {
        int tblCmp = tableId == o.tableId ? 0 : tableId.compareTo(o.tableId);

        if (tblCmp != 0) {
            return tblCmp;
        }

        return Integer.compare(partId, o.partId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ReplicationGroupId that = (ReplicationGroupId) o;

        return partId == that.partId && (tableId == that.tableId || tableId.equals(that.tableId));
    }

    @Override
    public int hashCode() {
        return tableId != null ? tableId.hashCode() ^ partId : partId;
    }

    @Override
    public String toString() {
        return tableId + "_part_" + partId;
    }
}
