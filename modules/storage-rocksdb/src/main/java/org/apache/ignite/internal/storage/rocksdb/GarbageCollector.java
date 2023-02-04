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

package org.apache.ignite.internal.storage.rocksdb;

import static java.lang.ThreadLocal.withInitial;
import static java.nio.ByteBuffer.allocateDirect;
import static org.apache.ignite.internal.hlc.HybridTimestamp.HYBRID_TIMESTAMP_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.Helper.KEY_BYTE_ORDER;
import static org.apache.ignite.internal.storage.rocksdb.Helper.MAX_KEY_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.Helper.MV_KEY_BUFFER;
import static org.apache.ignite.internal.storage.rocksdb.Helper.PARTITION_ID_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.Helper.ROW_ID_OFFSET;
import static org.apache.ignite.internal.storage.rocksdb.Helper.ROW_ID_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.Helper.TABLE_ROW_BYTE_ORDER;
import static org.apache.ignite.internal.storage.rocksdb.Helper.readTimestampNatural;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbMvPartitionStorage.invalid;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.TableRow;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.TableRowAndRowId;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteBatchWithIndex;

/**
 * Garbage collector.
 *
 * <p>Key:
 * <pre>{@code
 * | partId (2 bytes, BE) | timestamp (12 bytes, ASC) | rowId (16 bytes, BE) |
 * }</pre>
 * Value is an empty byte array.
 *
 * <p>For more information refer to the tech-notes/garbage-collection.md in this module.
 */
class GarbageCollector {
    /**
     * Empty direct byte buffer. Note that allocating memory of size 0 is UB, so java actually allocates
     * a 1-byte space. Be sure not to use this buffer for actual reading or writing.
     * In this instance it is only used for RocksDB to get the size of the entry without copying the entry into the buffer.
     */
    private static final ByteBuffer EMPTY_DIRECT_BUFFER = allocateDirect(0);

    /** Garbage collector's queue key's timestamp offset. */
    private static final int GC_KEY_TS_OFFSET = PARTITION_ID_SIZE;

    /** Garbage collector's queue key's row id offset. */
    private static final int GC_KEY_ROW_ID_OFFSET = GC_KEY_TS_OFFSET + HYBRID_TIMESTAMP_SIZE;

    /** Garbage collector's queue key's size. */
    private static final int GC_KEY_SIZE = GC_KEY_ROW_ID_OFFSET + ROW_ID_SIZE;

    /** Thread-local direct buffer instance to read keys from RocksDB. */
    private static final ThreadLocal<ByteBuffer> GC_KEY_BUFFER = withInitial(() -> allocateDirect(GC_KEY_SIZE).order(KEY_BYTE_ORDER));

    /** Helper for the rocksdb partition. */
    private final Helper helper;

    GarbageCollector(Helper helper) {
        this.helper = helper;
    }

    /**
     * Tries adding a row to the GC queue. We put new row's timestamp, because we can remove previous row only if both this row's
     * and previous row's timestamps are below the watermark.
     * Returns {@code true} if new value and previous value are both tombstones.
     *
     * @param writeBatch Write batch.
     * @param rowId Row id.
     * @param timestamp New row's timestamp.
     * @param isNewValueTombstone If new row is a tombstone.
     * @return {@code true} if new value and previous value are both tombstones.
     * @throws RocksDBException If failed.
     */
    boolean tryAddToGcQueue(WriteBatchWithIndex writeBatch, RowId rowId, HybridTimestamp timestamp, boolean isNewValueTombstone)
            throws RocksDBException {
        RocksDB db = helper.db;
        ColumnFamilyHandle gc = helper.gc;
        ColumnFamilyHandle cf = helper.cf;

        boolean newAndPrevTombstones = false;

        // Try find previous value for the row id.
        ByteBuffer keyBuffer = MV_KEY_BUFFER.get();
        keyBuffer.clear();

        helper.putDataKey(keyBuffer, rowId, timestamp);

        try (
                Slice upperBound = new Slice(helper.partitionEndPrefix());
                ReadOptions readOpts = new ReadOptions().setTotalOrderSeek(true).setIterateUpperBound(upperBound);
                RocksIterator it = db.newIterator(cf, readOpts)
        ) {
            it.seek(keyBuffer);

            if (invalid(it)) {
                return false;
            }

            keyBuffer.clear();

            int keyLen = it.key(keyBuffer);

            RowId readRowId = helper.getRowId(keyBuffer, ROW_ID_OFFSET);

            if (readRowId.equals(rowId)) {
                // Found previous value.
                assert keyLen == MAX_KEY_SIZE; // Can not be write-intent.

                if (isNewValueTombstone) {
                    // If new value is a tombstone, lets check if previous value was also a tombstone.
                    int valueSize = it.value(EMPTY_DIRECT_BUFFER);

                    newAndPrevTombstones = valueSize == 0;
                }

                if (!newAndPrevTombstones) {
                    keyBuffer.clear();

                    helper.putGcKey(keyBuffer, rowId, timestamp);

                    writeBatch.put(gc, keyBuffer, EMPTY_DIRECT_BUFFER);
                }
            }
        }

        return newAndPrevTombstones;
    }

    /**
     * Polls an element for vacuum. See {@link org.apache.ignite.internal.storage.MvPartitionStorage#pollForVacuum(HybridTimestamp)}.
     *
     * @param batch Write batch.
     * @param lowWatermark Low watermark.
     * @return Garbage collected element.
     * @throws RocksDBException If failed to collect the garbage.
     */
    @Nullable TableRowAndRowId pollForVacuum(WriteBatchWithIndex batch, HybridTimestamp lowWatermark) throws RocksDBException {
        RocksDB db = helper.db;
        ColumnFamilyHandle gc = helper.gc;
        ColumnFamilyHandle cf = helper.cf;

        // We retrieve the first element of the GC queue and seek for it in the data CF.
        // However, the element that we need to garbage collect is the next (older one) element.
        // First we check if there's anything to garbage collect. If the element is a tombstone we remove it.
        // If the next element exists, that should be the element that we want to garbage collect.
        try (
                var gcUpperBound = new Slice(helper.partitionEndPrefix());
                ReadOptions gcOpts = new ReadOptions().setIterateUpperBound(gcUpperBound);
                RocksIterator gcIt = db.newIterator(gc, gcOpts)
        ) {
            gcIt.seek(helper.partitionStartPrefix());

            if (invalid(gcIt)) {
                // GC queue is empty.
                return null;
            }

            ByteBuffer gcKeyBuffer = GC_KEY_BUFFER.get();
            gcKeyBuffer.clear();

            gcIt.key(gcKeyBuffer);

            HybridTimestamp gcElementTimestamp = readTimestampNatural(gcKeyBuffer, GC_KEY_TS_OFFSET);

            if (gcElementTimestamp.compareTo(lowWatermark) > 0) {
                // No elements to garbage collect.
                return null;
            }

            RowId gcElementRowId = helper.getRowId(gcKeyBuffer, GC_KEY_ROW_ID_OFFSET);

            // Delete element from the GC queue.
            batch.delete(gc, gcKeyBuffer);

            try (
                    var upperBound = new Slice(helper.partitionEndPrefix());
                    ReadOptions opts = new ReadOptions().setIterateUpperBound(upperBound);
                    RocksIterator it = db.newIterator(cf, opts)
            ) {
                ByteBuffer dataKeyBuffer = MV_KEY_BUFFER.get();
                dataKeyBuffer.clear();

                // Process the element in data cf that triggered the addition to the GC queue.
                boolean proceed = checkHasNewerRowAndRemoveTombstone(it, batch, dataKeyBuffer, gcElementRowId, gcElementTimestamp);

                if (!proceed) {
                    // No further processing required.
                    return null;
                }

                // Process the element in data cf that should be garbage collected.
                proceed = checkHasRowForGc(it, dataKeyBuffer, gcElementRowId);

                if (!proceed) {
                    // No further processing required.
                    return null;
                }

                // At this point there's definitely a value that needs to be garbage collected in the iterator.
                byte[] valueBytes = it.value();

                var row = new TableRow(ByteBuffer.wrap(valueBytes).order(TABLE_ROW_BYTE_ORDER));
                TableRowAndRowId retVal = new TableRowAndRowId(row, gcElementRowId);

                // Delete the row from the data cf.
                batch.delete(cf, dataKeyBuffer);

                return retVal;
            }
        }
    }

    /**
     * Processes the entry that triggered adding row id to garbage collector's queue.
     * <br>
     * There might already be no row in the data column family, because GC can be run in parallel.
     * If there is no row in the data column family, returns {@code false} as no further processing is required.
     * if there is a row and this entry is a tombstone, removes tombstone.
     *
     * @param it RocksDB data column family iterator.
     * @param batch Write batch.
     * @param dataKeyBuffer Buffer for the data column family key.
     * @param gcElementRowId Row id of the element from the GC queue/
     * @return {@code true} if further processing by garbage collector is needed.
     */
    private boolean checkHasNewerRowAndRemoveTombstone(RocksIterator it, WriteBatchWithIndex batch, ByteBuffer dataKeyBuffer,
            RowId gcElementRowId, HybridTimestamp gcElementTimestamp) throws RocksDBException {
        ColumnFamilyHandle cf = helper.cf;

        // Set up the data key.
        helper.putDataKey(dataKeyBuffer, gcElementRowId, gcElementTimestamp);

        // Seek to the row id and timestamp from the GC queue.
        // Note that it doesn't mean that the element in this iterator has matching row id or even partition id.
        it.seek(dataKeyBuffer);

        boolean hasRowForGc = true;

        if (invalid(it)) {
            // There is no row for the GC queue element.
            hasRowForGc = false;
        } else {
            dataKeyBuffer.clear();

            it.key(dataKeyBuffer);

            if (!helper.getRowId(dataKeyBuffer, ROW_ID_OFFSET).equals(gcElementRowId)) {
                // There is no row for the GC queue element.
                hasRowForGc = false;
            }
        }

        if (!hasRowForGc) {
            return false;
        }

        // Check if the new element, whose insertion scheduled the GC, was a tombstone.
        int len = it.value(EMPTY_DIRECT_BUFFER);

        if (len == 0) {
            // This is a tombstone, we need to delete it.
            batch.delete(cf, dataKeyBuffer);
        }

        return true;
    }

    /**
     * Checks if there is a row for garbage collection.
     * There might already be no row in the data column family, because GC can be run in parallel.
     *
     * @param it RocksDB data column family iterator.
     * @param dataKeyBuffer Buffer for the data column family key.
     * @param gcElementRowId Row id of the element from the GC queue/
     * @return {@code true} if further processing by garbage collector is needed.
     */
    private boolean checkHasRowForGc(RocksIterator it, ByteBuffer dataKeyBuffer, RowId gcElementRowId) {
        // Let's move to the element that was scheduled for GC.
        it.next();

        boolean hasRowForGc = true;

        RowId gcRowId;

        if (invalid(it)) {
            hasRowForGc = false;
        } else {
            dataKeyBuffer.clear();

            int keyLen = it.key(dataKeyBuffer);

            if (keyLen != MAX_KEY_SIZE) {
                // We moved to the next row id's write-intent, so there was no row to GC for the current row id.
                hasRowForGc = false;
            } else {
                gcRowId = helper.getRowId(dataKeyBuffer, ROW_ID_OFFSET);

                if (!gcElementRowId.equals(gcRowId)) {
                    // We moved to the next row id, so there was no row to GC for the current row id.
                    hasRowForGc = false;
                }
            }
        }

        return hasRowForGc;
    }

    /**
     * Deletes garbage collector's queue.
     *
     * @param writeBatch Write batch.
     * @throws RocksDBException If failed to delete the queue.
     */
    void deleteQueue(WriteBatch writeBatch) throws RocksDBException {
        writeBatch.deleteRange(helper.gc, helper.partitionStartPrefix(), helper.partitionEndPrefix());
    }
}
