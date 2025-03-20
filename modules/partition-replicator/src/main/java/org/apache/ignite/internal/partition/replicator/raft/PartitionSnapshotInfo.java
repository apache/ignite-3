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

package org.apache.ignite.internal.partition.replicator.raft;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Class containing information about the last performed Raft snapshot.
 */
public class PartitionSnapshotInfo {
    private final long lastAppliedIndex;

    private final long lastAppliedTerm;

    @Nullable
    private final LeaseInfo leaseInfo;

    private final byte[] configurationBytes;

    private final Set<Integer> tableIds;

    /**
     * Constructor.
     *
     * @param lastAppliedIndex Applied index at the moment when the snapshot was taken.
     * @param lastAppliedTerm Applied term at the moment when the snapshot was taken.
     * @param leaseInfo Lease information or {@code null} if no lease has been issued for this storage yet.
     * @param configurationBytes Serialized representation of Raft group configuration.
     * @param tableIds IDs of tables that were part of the Raft group when the snapshot was taken.
     */
    public PartitionSnapshotInfo(
            long lastAppliedIndex,
            long lastAppliedTerm,
            @Nullable LeaseInfo leaseInfo,
            byte[] configurationBytes,
            Collection<Integer> tableIds
    ) {
        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedTerm = lastAppliedTerm;
        this.leaseInfo = leaseInfo;
        this.configurationBytes = configurationBytes;
        this.tableIds = Set.copyOf(tableIds);
    }

    public long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    public long lastAppliedTerm() {
        return lastAppliedTerm;
    }

    @Nullable
    public LeaseInfo leaseInfo() {
        return leaseInfo;
    }

    public byte[] configurationBytes() {
        return configurationBytes;
    }

    /**
     * Returns the set of table IDs which partition storages took part in this Raft snapshot of a zone-wide Raft group.
     *
     * <p>If a table ID is seen in this set, it effectively means that this table's partition storage has been durably flushed and is
     * guaranteed to have seen the state of the Raft group at the moment when the snapshot was taken. This information is then used during
     * recovery to determine whether a table's partition storage has suffered data loss or simply hasn't had any updates yet.
     */
    public Set<Integer> tableIds() {
        return tableIds;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PartitionSnapshotInfo that = (PartitionSnapshotInfo) o;
        return lastAppliedIndex == that.lastAppliedIndex && lastAppliedTerm == that.lastAppliedTerm && Objects.equals(leaseInfo,
                that.leaseInfo) && Arrays.equals(configurationBytes, that.configurationBytes) && tableIds.equals(that.tableIds);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(lastAppliedIndex);
        result = 31 * result + Long.hashCode(lastAppliedTerm);
        result = 31 * result + Objects.hashCode(leaseInfo);
        result = 31 * result + Arrays.hashCode(configurationBytes);
        result = 31 * result + tableIds.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
