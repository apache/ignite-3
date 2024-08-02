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
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.DATA_ID_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.MAX_KEY_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.ROW_ID_OFFSET;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.deserializeRow;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.getFromBatchAndDb;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.isTombstone;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.readTimestampNatural;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.wrapIterator;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbMvPartitionStorage.invalid;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.KEY_BYTE_ORDER;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.PARTITION_ID_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.ROW_ID_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.TABLE_ID_SIZE;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.gc.GcEntry;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteBatchWithIndex;

/**
 * Garbage collector.
 *
 * <p>Key:
 * <pre>{@code
 * | tableId (4 bytes, BE) | partId (2 bytes, BE) | timestamp (8 bytes, ASC) | rowId (16 bytes, BE) |
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
    private static final int GC_KEY_TS_OFFSET = TABLE_ID_SIZE + PARTITION_ID_SIZE;

    /** Garbage collector's queue key's row id offset. */
    private static final int GC_KEY_ROW_ID_OFFSET = GC_KEY_TS_OFFSET + HYBRID_TIMESTAMP_SIZE;

    /** Garbage collector's queue key's size. */
    private static final int GC_KEY_SIZE = GC_KEY_ROW_ID_OFFSET + ROW_ID_SIZE;

    /** Thread-local direct buffer instance to read keys that contain Data ID. */
    private static final ThreadLocal<ByteBuffer> DIRECT_DATA_ID_KEY_BUFFER =
            withInitial(() -> allocateDirect(MAX_KEY_SIZE).order(KEY_BYTE_ORDER));

    /** Thread-local direct buffer for storing Data ID. */
    private static final ThreadLocal<ByteBuffer> DIRECT_DATA_ID_BUFFER =
            withInitial(() -> allocateDirect(DATA_ID_SIZE).order(KEY_BYTE_ORDER));

    /** Thread-local direct buffer instance to read keys from the GC queue. */
    private static final ThreadLocal<ByteBuffer> DIRECT_GC_KEY_BUFFER =
            withInitial(() -> allocateDirect(GC_KEY_SIZE).order(KEY_BYTE_ORDER));

    /** Helper for the rocksdb partition. */
    private final PartitionDataHelper helper;

    /** RocksDB instance. */
    private final RocksDB db;

    /** GC queue column family. */
    private final ColumnFamilyHandle gcQueueCf;

    /** Read options for regular reads. */
    private final ReadOptions readOpts;

    enum AddResult {
        WAS_TOMBSTONE, WAS_VALUE, WAS_EMPTY
    }

    GarbageCollector(PartitionDataHelper helper, RocksDB db, ReadOptions readOpts, ColumnFamilyHandle gcQueueCf) {
        this.helper = helper;
        this.db = db;
        this.gcQueueCf = gcQueueCf;
        this.readOpts = readOpts;
    }

    /**
     * Tries adding a row to the GC queue. We put new row's timestamp, because we can remove previous row only if both this row's
     * and previous row's timestamps are below the watermark.
     *
     * @param writeBatch Write batch.
     * @param rowId Row id.
     * @param timestamp New row's timestamp.
     * @param isNewValueTombstone If new row is a tombstone.
     * @return Enum indicating the type of the previous value that has been replaced.
     * @throws RocksDBException If failed.
     */
    AddResult tryAddToGcQueue(WriteBatchWithIndex writeBatch, RowId rowId, HybridTimestamp timestamp, boolean isNewValueTombstone)
            throws RocksDBException {
        // Try find previous value for the row id.
        ByteBuffer dataIdKeyBuffer = DIRECT_DATA_ID_KEY_BUFFER.get().clear();

        helper.putCommittedDataIdKey(dataIdKeyBuffer, rowId, timestamp);

        try (RocksIterator it = db.newIterator(helper.partCf, helper.upperBoundReadOpts)) {
            it.seek(dataIdKeyBuffer);

            if (invalid(it)) {
                return AddResult.WAS_EMPTY;
            }

            dataIdKeyBuffer.clear();

            int keyLen = it.key(dataIdKeyBuffer);

            RowId readRowId = helper.getRowId(dataIdKeyBuffer, ROW_ID_OFFSET);

            if (!readRowId.equals(rowId)) {
                return AddResult.WAS_EMPTY;
            }

            // Found previous value.
            assert keyLen == MAX_KEY_SIZE; // Can not be write-intent.

            AddResult result;

            if (isCurrentValueTombstone(it)) {
                // Do not add a new tombstone if the existing value is also a tombstone.
                if (isNewValueTombstone) {
                    return AddResult.WAS_TOMBSTONE;
                }

                result = AddResult.WAS_TOMBSTONE;
            } else {
                result = AddResult.WAS_VALUE;
            }

            ByteBuffer gcKeyBuffer = DIRECT_GC_KEY_BUFFER.get().clear();

            helper.putGcKey(gcKeyBuffer, rowId, timestamp);

            writeBatch.put(gcQueueCf, gcKeyBuffer, EMPTY_DIRECT_BUFFER);

            return result;
        }
    }

    /**
     * Polls an element for vacuum. See {@link org.apache.ignite.internal.storage.MvPartitionStorage#peek(HybridTimestamp)}.
     *
     * @param lowWatermark Low watermark.
     * @return Garbage collected element descriptor.
     */
    @Nullable GcEntry peek(WriteBatchWithIndex writeBatch, HybridTimestamp lowWatermark) {
        // We retrieve the first element of the GC queue and seek for it in the data CF.
        // However, the element that we need to garbage collect is the next (older one) element.
        // First we check if there's anything to garbage collect. If the element is a tombstone we remove it.
        // If the next element exists, that should be the element that we want to garbage collect.
        try (RocksIterator gcIt = newWrappedIterator(writeBatch, gcQueueCf, helper.upperBoundReadOpts)) {
            gcIt.seek(helper.partitionStartPrefix());

            if (invalid(gcIt)) {
                // GC queue is empty.
                return null;
            }

            ByteBuffer gcKeyBuffer = readGcKey(gcIt);

            GcRowVersion gcRowVersion = toGcRowVersion(gcKeyBuffer);

            if (gcRowVersion.getTimestamp().compareTo(lowWatermark) > 0) {
                // No elements to garbage collect.
                return null;
            }

            return gcRowVersion;
        }
    }


    /**
     * Polls an element for vacuum. See {@link org.apache.ignite.internal.storage.MvPartitionStorage#vacuum(GcEntry)}.
     *
     * @param batch Write batch.
     * @param entry Entry, previously returned by {@link #peek}.
     * @return Garbage collected element.
     * @throws RocksDBException If failed to collect the garbage.
     */
    @Nullable BinaryRow vacuum(WriteBatchWithIndex batch, GcEntry entry) throws RocksDBException {
        assert entry instanceof GcRowVersion;

        ColumnFamilyHandle partCf = helper.partCf;

        // We retrieve the first element of the GC queue and seek for it in the data CF.
        // However, the element that we need to garbage collect is the next (older one) element.
        // First we check if there's anything to garbage collect. If the element is a tombstone we remove it.
        // If the next element exists, that should be the element that we want to garbage collect.
        try (RocksIterator gcIt = newWrappedIterator(batch, gcQueueCf, helper.upperBoundReadOpts)) {
            gcIt.seek(helper.partitionStartPrefix());

            if (invalid(gcIt)) {
                // GC queue is empty.
                return null;
            }

            ByteBuffer gcKeyBuffer = readGcKey(gcIt);

            GcRowVersion gcRowVersion = toGcRowVersion(gcKeyBuffer);

            // Someone has processed the element in parallel, so we need to take a new head of the queue.
            if (!gcRowVersion.equals(entry)) {
                return null;
            }

            // Delete element from the GC queue.
            batch.delete(gcQueueCf, gcKeyBuffer);

            try (RocksIterator partIt = newWrappedIterator(batch, partCf, helper.upperBoundReadOpts)) {
                // Process the element in data cf that triggered the addition to the GC queue.
                boolean proceed = checkHasNewerRowAndRemoveTombstone(partIt, batch, gcRowVersion);

                if (!proceed) {
                    // No further processing required.
                    return null;
                }

                // Find the row that should be garbage collected.
                ByteBuffer dataIdKey = getDataIdForGcKey(partIt, gcRowVersion.getRowId());

                if (dataIdKey == null) {
                    // No row for GC.
                    return null;
                }

                // At this point there's definitely a value that needs to be garbage collected in the iterator.
                ByteBuffer dataId = readDataId(partIt);

                assert !isTombstone(dataId);

                byte[] payloadKey = helper.createPayloadKey(dataId);

                byte[] rowBytes = getFromBatchAndDb(db, batch, helper.dataCf, readOpts, payloadKey);

                assert rowBytes != null && rowBytes.length > 0;

                // Delete the row from the data cf.
                batch.delete(partCf, dataIdKey);
                batch.delete(helper.dataCf, payloadKey);

                return deserializeRow(rowBytes);
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
     * @param gcRowVersion Row version from the GC queue.
     * @return {@code true} if further processing by garbage collector is needed.
     */
    private boolean checkHasNewerRowAndRemoveTombstone(
            RocksIterator it,
            WriteBatchWithIndex batch,
            GcRowVersion gcRowVersion
    ) throws RocksDBException {
        ByteBuffer dataIdKeyBuffer = DIRECT_DATA_ID_KEY_BUFFER.get().clear();

        // Set up the data key.
        helper.putCommittedDataIdKey(dataIdKeyBuffer, gcRowVersion.getRowId(), gcRowVersion.getTimestamp());

        // Seek to the row id and timestamp from the GC queue.
        // Note that it doesn't mean that the element in this iterator has matching row id or even partition id.
        it.seek(dataIdKeyBuffer);

        if (invalid(it)) {
            // There is no row for the GC queue element.
            return false;
        }

        dataIdKeyBuffer.clear();

        it.key(dataIdKeyBuffer);

        if (!helper.getRowId(dataIdKeyBuffer, ROW_ID_OFFSET).equals(gcRowVersion.getRowId())) {
            // There is no row for the GC queue element.
            return false;
        }

        // Check if the new element, whose insertion scheduled the GC, was a tombstone.
        if (isCurrentValueTombstone(it)) {
            // This is a tombstone, we need to delete it.
            batch.delete(helper.partCf, dataIdKeyBuffer);
        }

        return true;
    }

    /**
     * Checks if there is a row for garbage collection and returns this row's key if it exists.
     * There might already be no row in the data column family, because GC can be run in parallel.
     *
     * @param it RocksDB data column family iterator.
     * @param gcElementRowId Row id of the element from the GC queue/
     * @return Key of the row that needs to be garbage collected, or {@code null} if such row doesn't exist.
     */
    private @Nullable ByteBuffer getDataIdForGcKey(RocksIterator it, RowId gcElementRowId) {
        // Let's move to the element that was scheduled for GC.
        it.next();

        if (invalid(it)) {
            return null;
        }

        ByteBuffer dataIdKeyBuffer = DIRECT_DATA_ID_KEY_BUFFER.get().clear();

        int keyLen = it.key(dataIdKeyBuffer);

        // Check if we moved to another row id's write-intent, that would mean that there is no row to GC for the current row id.
        if (keyLen == MAX_KEY_SIZE) {
            RowId rowId = helper.getRowId(dataIdKeyBuffer, ROW_ID_OFFSET);

            // We might have moved to the next row id.
            if (gcElementRowId.equals(rowId)) {
                return dataIdKeyBuffer;
            }
        }

        return null;
    }

    /**
     * Deletes garbage collector's queue.
     *
     * @param writeBatch Write batch.
     * @throws RocksDBException If failed to delete the queue.
     */
    void deleteQueue(WriteBatch writeBatch) throws RocksDBException {
        writeBatch.deleteRange(gcQueueCf, helper.partitionStartPrefix(), helper.partitionEndPrefix());
    }

    private static ByteBuffer readGcKey(RocksIterator gcIt) {
        ByteBuffer gcKeyBuffer = DIRECT_GC_KEY_BUFFER.get().clear();

        gcIt.key(gcKeyBuffer);

        return gcKeyBuffer;
    }

    private GcRowVersion toGcRowVersion(ByteBuffer gcKeyBuffer) {
        return new GcRowVersion(
                helper.getRowId(gcKeyBuffer, GC_KEY_ROW_ID_OFFSET),
                readTimestampNatural(gcKeyBuffer, GC_KEY_TS_OFFSET)
        );
    }

    private RocksIterator newWrappedIterator(WriteBatchWithIndex writeBatch, ColumnFamilyHandle cf, ReadOptions readOptions) {
        RocksIterator it = db.newIterator(cf, readOptions);

        return wrapIterator(it, writeBatch, cf);
    }

    private static ByteBuffer readDataId(RocksIterator it) {
        ByteBuffer dataId = DIRECT_DATA_ID_BUFFER.get().clear();

        it.value(dataId);

        return dataId;
    }

    private static boolean isCurrentValueTombstone(RocksIterator it) {
        return isTombstone(readDataId(it));
    }
}
