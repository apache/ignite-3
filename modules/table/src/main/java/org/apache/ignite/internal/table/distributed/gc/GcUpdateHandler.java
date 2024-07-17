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
     * @param strict {@code true} if needed to remove the strictly passed {@code count} oldest stale entries, {@code false} if a premature
     *      exit is allowed when it is not possible to acquire a lock for the {@link RowId}.
     * @return {@code False} if there is no garbage left in the storage.
     */
    public boolean vacuumBatch(HybridTimestamp lowWatermark, int count, boolean strict) {
        if (count <= 0) {
            return true;
        }

        IntHolder countHolder = new IntHolder(count);

        while (countHolder.get() > 0) {
            VacuumResult vacuumResult = internalVacuumBatch(lowWatermark, countHolder);

            switch (vacuumResult) {
                case NO_GARBAGE_LEFT:
                    return false;
                case SUCCESS:
                    return true;
                case FAILED_ACQUIRE_LOCK:
                    if (strict) {
                        continue;
                    }

                    return true;
                default:
                    throw new IllegalStateException(vacuumResult.toString());
            }
        }

        return true;
    }

    private VacuumResult internalVacuumBatch(HybridTimestamp lowWatermark, IntHolder countHolder) {
        return storage.runConsistently(locker -> {
            int count = countHolder.get();

            for (int i = 0; i < count; i++) {
                // It is safe for the first iteration to use a lock instead of tryLock, since there will be no deadlock for the first RowId
                // and a deadlock may happen with subsequent ones.
                VacuumResult vacuumResult = internalVacuum(lowWatermark, locker, i > 0);

                if (vacuumResult != VacuumResult.SUCCESS) {
                    return vacuumResult;
                }

                countHolder.getAndDecrement();
            }

            return VacuumResult.SUCCESS;
        });
    }

    private VacuumResult internalVacuum(HybridTimestamp lowWatermark, Locker locker, boolean useTryLock) {
        while (true) {
            GcEntry gcEntry = storage.peek(lowWatermark);

            if (gcEntry == null) {
                return VacuumResult.NO_GARBAGE_LEFT;
            }

            RowId rowId = gcEntry.getRowId();

            if (useTryLock) {
                if (!locker.tryLock(rowId)) {
                    return VacuumResult.FAILED_ACQUIRE_LOCK;
                }
            } else {
                locker.lock(rowId);
            }

            BinaryRow binaryRow = storage.vacuum(gcEntry);

            if (binaryRow == null) {
                // Removed by another thread, let's try to take another.
                continue;
            }

            try (Cursor<ReadResult> cursor = storage.scanVersions(rowId)) {
                // TODO: IGNITE-21005 We need to choose only those indexes that are not available for transactions
                indexUpdateHandler.tryRemoveFromIndexes(binaryRow, rowId, cursor, null);
            }

            return VacuumResult.SUCCESS;
        }
    }

    private enum VacuumResult {
        SUCCESS, NO_GARBAGE_LEFT, FAILED_ACQUIRE_LOCK
    }
}
