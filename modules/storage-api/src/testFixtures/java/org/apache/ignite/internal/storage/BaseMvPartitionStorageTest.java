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

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.gc.GcEntry;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;

/**
 * Base test for MV partition storages.
 */
public abstract class BaseMvPartitionStorageTest extends BaseMvStoragesTest {
    protected static final int PARTITION_ID = 1;

    protected static final int COMMIT_ZONE_ID = 999;

    protected static final RowId ROW_ID = new RowId(PARTITION_ID);

    protected static final TestKey KEY = new TestKey(10, "foo");

    protected static final BinaryRow TABLE_ROW = binaryRow(KEY, new TestValue(20, "bar"));

    protected static final BinaryRow TABLE_ROW2 = binaryRow(KEY, new TestValue(30, "bar"));

    protected static final BinaryRow TABLE_ROW3 = binaryRow(KEY, new TestValue(40, "baz"));

    protected MvPartitionStorage storage;

    @AfterEach
    protected void tearDown() throws Exception {
        IgniteUtils.closeAllManually(storage);
    }

    /**
     * Initializes the internal structures needed for tests.
     *
     * <p>This method *MUST* always be called in either subclass' constructor or setUp method.
     */
    protected final void initialize(MvTableStorage tableStorage) {
        storage = getOrCreateMvPartition(tableStorage, PARTITION_ID);
    }

    /**
     * Reads a row.
     */
    @Nullable
    protected BinaryRow read(RowId rowId, HybridTimestamp timestamp) {
        ReadResult readResult = storage.read(rowId, timestamp);

        return readResult.binaryRow();
    }

    /**
     * Scans partition.
     */
    protected PartitionTimestampCursor scan(HybridTimestamp timestamp) {
        return storage.scan(timestamp);
    }

    /**
     * Scans versions.
     */
    protected Cursor<ReadResult> scan(RowId rowId) {
        return storage.scanVersions(rowId);
    }

    /**
     * Inserts a row inside of consistency closure.
     */
    protected RowId insert(@Nullable BinaryRow binaryRow, UUID txId) {
        RowId rowId = new RowId(PARTITION_ID);

        addWrite(rowId, binaryRow, txId);

        return rowId;
    }

    /**
     * Adds/updates a write-intent inside of consistency closure.
     */
    protected AddWriteResult addWrite(RowId rowId, @Nullable BinaryRow binaryRow, UUID txId) {
        return storage.runConsistently(locker -> {
            locker.lock(rowId);

            return storage.addWrite(rowId, binaryRow, txId, COMMIT_ZONE_ID, PARTITION_ID);
        });
    }

    /**
     * Commits write-intent inside of consistency closure.
     */
    protected CommitResult commitWrite(RowId rowId, HybridTimestamp tsExact, UUID txId) {
        return storage.runConsistently(locker -> {
            locker.lock(rowId);

            return storage.commitWrite(rowId, tsExact, txId);
        });
    }

    /**
     * Creates a committed version inside of consistency closure.
     */
    protected AddWriteCommittedResult addWriteCommitted(RowId rowId, @Nullable BinaryRow row, HybridTimestamp commitTimestamp) {
        return storage.runConsistently(locker -> {
            locker.lock(rowId);

            return storage.addWriteCommitted(rowId, row, commitTimestamp);
        });
    }

    protected HybridTimestamp addAndCommit(@Nullable BinaryRow binaryRow) {
        HybridTimestamp commitTs = clock.now();
        UUID txId = newTransactionId();

        addWrite(ROW_ID, binaryRow, txId);
        commitWrite(ROW_ID, commitTs, txId);

        return commitTs;
    }

    /**
     * Aborts write-intent inside of consistency closure.
     */
    protected AbortResult abortWrite(RowId rowId, UUID txId) {
        return storage.runConsistently(locker -> {
            locker.lock(rowId);

            return storage.abortWrite(rowId, txId);
        });
    }

    /**
     * Polls the oldest row in the partition, removing it at the same time.
     *
     * @param lowWatermark A time threshold for the row. Only rows that have versions with timestamp higher or equal to the watermark
     *      can be removed.
     * @return A pair of table row and row id, where a timestamp of the row is less than or equal to {@code lowWatermark}.
     *      {@code null} if there's no such value.
     */
    @Nullable BinaryRowAndRowId pollForVacuum(HybridTimestamp lowWatermark) {
        while (true) {
            BinaryRowAndRowId binaryRowAndRowId = storage.runConsistently(locker -> {
                List<GcEntry> gcEntries = storage.peek(lowWatermark, 1);

                if (gcEntries.isEmpty()) {
                    return null;
                }

                GcEntry gcEntry = gcEntries.get(0);

                locker.lock(gcEntry.getRowId());

                return new BinaryRowAndRowId(storage.vacuum(gcEntry), gcEntry.getRowId());
            });

            if (binaryRowAndRowId == null) {
                return null;
            }

            if (binaryRowAndRowId.binaryRow() == null) {
                continue;
            }

            return binaryRowAndRowId;
        }
    }
}
