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

package org.apache.ignite.internal.table.distributed.gc;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.BinaryRowAndRowId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;

/**
 * Garbage collection update handler.
 */
public class GcUpdateHandler {
    private final PartitionDataStorage storage;

    private final IndexUpdateHandler indexUpdateHandler;

    private final PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker;

    /**
     * Constructor.
     *
     * @param storage Partition data storage.
     * @param indexUpdateHandler Index update handler.
     * @param safeTimeTracker Partition safe time tracker.
     */
    public GcUpdateHandler(
            PartitionDataStorage storage,
            PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker,
            IndexUpdateHandler indexUpdateHandler
    ) {
        this.storage = storage;
        this.indexUpdateHandler = indexUpdateHandler;
        this.safeTimeTracker = safeTimeTracker;
    }

    /**
     * Returns the partition safe time tracker.
     */
    public PendingComparableValuesTracker<HybridTimestamp, Void> getSafeTimeTracker() {
        return safeTimeTracker;
    }

    /**
     * Tries removing partition's oldest stale entry and its indexes.
     *
     * @param lowWatermark Low watermark for the vacuum.
     * @return {@code true} if an entry was garbage collected, {@code false} if there was nothing to collect.
     * @see MvPartitionStorage#pollForVacuum(HybridTimestamp)
     */
    public boolean vacuum(HybridTimestamp lowWatermark) {
        return storage.runConsistently(locker -> internalVacuum(lowWatermark));
    }

    /**
     * Executes garbage collection.
     *
     * <p>Must be called inside a {@link MvPartitionStorage#runConsistently(WriteClosure)} closure.
     *
     * @param lowWatermark Low watermark for the vacuum.
     * @return {@code true} if an entry was garbage collected, {@code false} if there was nothing to collect.
     */
    private boolean internalVacuum(HybridTimestamp lowWatermark) {
        BinaryRowAndRowId vacuumed = storage.pollForVacuum(lowWatermark);

        if (vacuumed == null) {
            // Nothing was garbage collected.
            return false;
        }

        BinaryRow binaryRow = vacuumed.binaryRow();

        assert binaryRow != null;

        RowId rowId = vacuumed.rowId();

        try (Cursor<ReadResult> cursor = storage.scanVersions(rowId)) {
            indexUpdateHandler.tryRemoveFromIndexes(binaryRow, rowId, cursor);
        }

        return true;
    }
}
