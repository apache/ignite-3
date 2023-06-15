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
import org.apache.ignite.internal.storage.MvPartitionStorage.Locker;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.gc.GcEntry;
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
     * Tries removing {@code count} oldest stale entries and their indexes.
     * If there are fewer rows than the {@code count}, then exits prematurely.
     *
     * @param lowWatermark Low watermark for the vacuum.
     * @param count Count of entries to GC.
     * @return {@code False} if there is no garbage left in the storage.
     */
    // TODO: IGNITE-19737 реализовать и поправить документацию
    public boolean vacuumBatch(HybridTimestamp lowWatermark, int count, boolean strict) {
        return storage.runConsistently(locker -> {
            for (int i = 0; i < count; i++) {
                if (!internalVacuum(lowWatermark, locker)) {
                    return false;
                }
            }

            return true;
        });
    }

    public boolean vacuumBatch0(HybridTimestamp lowWatermark, int count, boolean strict) {
        return storage.runConsistently(locker -> {
            for (int i = 0; i < count; i++) {
                if (!internalVacuum(lowWatermark, locker)) {
                    return false;
                }
            }

            return true;
        });
    }

    /**
     * Attempts to collect garbage for one {@link RowId}.
     *
     * <p>Must be called inside a {@link PartitionDataStorage#runConsistently(WriteClosure)} closure.
     *
     * @param lowWatermark Low watermark for the vacuum.
     * @param locker From {@link PartitionDataStorage#runConsistently(WriteClosure)}.
     * @return {@code False} if there is no garbage left in the {@link #storage}.
     */
    private boolean internalVacuum(HybridTimestamp lowWatermark, Locker locker) {
        while (true) {
            GcEntry gcEntry = storage.peek(lowWatermark);

            if (gcEntry == null) {
                return false;
            }

            RowId rowId = gcEntry.getRowId();

            if (!locker.tryLock(rowId)) {
                return true;
            }

            BinaryRow binaryRow = storage.vacuum(gcEntry);

            if (binaryRow == null) {
                // Removed by another thread, let's try to take another.
                continue;
            }

            try (Cursor<ReadResult> cursor = storage.scanVersions(rowId)) {
                indexUpdateHandler.tryRemoveFromIndexes(binaryRow, rowId, cursor);
            }

            return true;
        }
    }
}
