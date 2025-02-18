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

package org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionMvStorageAccess;
import org.apache.ignite.internal.storage.RowId;
import org.jetbrains.annotations.Nullable;

/**
 * Outgoing snapshot delivery state for a given partition.
 */
class MvPartitionDeliveryState {
    private final Iterator<PartitionMvStorageAccess> partitionStoragesIterator;

    /** Current row ID within the current partition storage. */
    @Nullable
    private RowId currentRowId;

    /** Current partition storage. */
    @Nullable
    private PartitionMvStorageAccess currentPartitionStorage;

    private final IntSet tableIds;

    private boolean isStarted = false;

    /**
     * Creates a new state. The state is initially positioned before the first row of the first storage.
     *
     * @param partitionStorages Partition storages to iterate over. They <b>must</b> be sorted by table ID in ascending order.
     */
    MvPartitionDeliveryState(List<PartitionMvStorageAccess> partitionStorages) {
        this.partitionStoragesIterator = partitionStorages.iterator();

        tableIds = new IntOpenHashSet(partitionStorages.size());

        partitionStorages.forEach(storage -> tableIds.add(storage.tableId()));
    }

    RowId currentRowId() {
        assert currentRowId != null;

        return currentRowId;
    }

    PartitionMvStorageAccess currentPartitionStorage() {
        assert currentPartitionStorage != null;

        return currentPartitionStorage;
    }

    int currentTableId() {
        return currentPartitionStorage().tableId();
    }

    /**
     * Returns {@code true} if the given table ID is in the range of tables that this state iterates over.
     */
    boolean isGoingToBeDelivered(int tableId) {
        return tableIds.contains(tableId);
    }

    /**
     * Returns {@code true} if all data for the snapshot has been retrieved.
     */
    boolean isExhausted() {
        return currentPartitionStorage == null && !partitionStoragesIterator.hasNext();
    }

    /**
     * Returns {@code true} if the {@link #advance()} method has been called at least once.
     */
    boolean hasIterationStarted() {
        return isStarted;
    }

    void advance() {
        isStarted = true;

        while (true) {
            if (currentPartitionStorage == null) {
                if (!partitionStoragesIterator.hasNext()) {
                    return;
                }

                currentPartitionStorage = partitionStoragesIterator.next();

                currentRowId = currentPartitionStorage.closestRowId(RowId.lowestRowId(currentPartitionStorage.partitionId()));
            } else {
                assert currentRowId != null;

                currentRowId = currentRowId.increment();

                if (currentRowId != null) {
                    currentRowId = currentPartitionStorage.closestRowId(currentRowId);
                }
            }

            if (currentRowId != null) {
                return;
            }

            // We've read all data from this partition, continue to the next one.
            currentPartitionStorage = null;
        }
    }
}
