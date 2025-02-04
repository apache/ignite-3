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

import java.util.Collection;
import java.util.Iterator;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionStorageAccess;
import org.apache.ignite.internal.storage.RowId;
import org.jetbrains.annotations.Nullable;

/**
 * Outgoing snapshot delivery state for a given partition.
 */
class PartitionDeliveryState {
    private final Iterator<PartitionStorageAccess> partitionStoragesIterator;

    /**
     * Current row ID within the current partition storage. Can be {@code null} only if the snapshot has delivered all possible data.
     */
    @Nullable
    private RowId currentRowId;

    /**
     * Current partition storage. Can be {@code null} only if the snapshot has delivered all possible data.
     */
    @Nullable
    private PartitionStorageAccess currentPartitionStorage;

    PartitionDeliveryState(Collection<PartitionStorageAccess> partitionStorages) {
        this.partitionStoragesIterator = partitionStorages.iterator();

        advance();
    }

    RowId currentRowId() {
        assert currentRowId != null;

        return currentRowId;
    }

    PartitionStorageAccess currentPartitionStorage() {
        assert currentPartitionStorage != null;

        return currentPartitionStorage;
    }

    int currentTableId() {
        return currentPartitionStorage().tableId();
    }

    boolean isEmpty() {
        return currentPartitionStorage == null;
    }

    void advance() {
        if (currentPartitionStorage == null) {
            if (!partitionStoragesIterator.hasNext()) {
                return;
            }

            currentPartitionStorage = partitionStoragesIterator.next();

            currentRowId = currentPartitionStorage.closestRowId(RowId.lowestRowId(currentPartitionStorage.partitionId()));

            // Partition is empty, try the next one.
            if (currentRowId == null) {
                moveToNextPartitionStorage();
            }
        } else {
            assert currentRowId != null;

            RowId nextRowId = currentRowId.increment();

            // We've exhausted all possible row IDs in the partition, switch to the next one.
            if (nextRowId == null) {
                moveToNextPartitionStorage();
            } else {
                currentRowId = currentPartitionStorage.closestRowId(nextRowId);

                // We've read all data from this partition, switch to the next one.
                if (currentRowId == null) {
                    moveToNextPartitionStorage();
                }
            }
        }
    }

    private void moveToNextPartitionStorage() {
        currentPartitionStorage = null;

        advance();
    }
}
