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

import static java.nio.ByteBuffer.allocate;
import static org.apache.ignite.internal.hlc.HybridTimestamp.HYBRID_TIMESTAMP_SIZE;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.rocksdb.RocksUtils.incrementPrefix;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.KEY_BYTE_ORDER;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.PARTITION_ID_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.ROW_ID_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.TABLE_ID_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.getRowIdUuid;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.putRowIdUuid;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowImpl;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.util.LockByRowId;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatchWithIndex;

/** Helper for the partition data. */
public final class PartitionDataHelper implements ManuallyCloseable {
    /** Transaction id size (part of the transaction state). */
    private static final int TX_ID_SIZE = 2 * Long.BYTES;

    /** Position of row id inside the key. */
    static final int ROW_ID_OFFSET = TABLE_ID_SIZE + PARTITION_ID_SIZE;

    /**
     * Part of the of the key associated with transaction state without the timestamp.
     * Also equal to the size of a key for an uncommitted Write Intent.
     */
    public static final int ROW_PREFIX_SIZE = ROW_ID_OFFSET + ROW_ID_SIZE;

    /** Maximum size of the key associated with transaction state. */
    static final int MAX_KEY_SIZE = ROW_PREFIX_SIZE + HYBRID_TIMESTAMP_SIZE;

    /** Size of Data ID. */
    static final int DATA_ID_SIZE = ROW_ID_SIZE + HYBRID_TIMESTAMP_SIZE;

    /** Offset of Data ID inside the key associated with row data. */
    private static final int DATA_ID_OFFSET = TABLE_ID_SIZE + PARTITION_ID_SIZE;

    /** Size of the key associated with row data. */
    private static final int PAYLOAD_KEY_SIZE = DATA_ID_OFFSET + DATA_ID_SIZE;

    /** Size of the transaction state part of the value (Tx ID + Commit Table ID + Commit Partition ID). */
    private static final int TX_STATE_SIZE = TX_ID_SIZE + TABLE_ID_SIZE + PARTITION_ID_SIZE;

    static final int DATA_ID_WITH_TX_STATE_SIZE = DATA_ID_SIZE + TX_STATE_SIZE;

    /** Thread-local write batch for {@link MvPartitionStorage#runConsistently(WriteClosure)}. */
    static final ThreadLocal<ThreadLocalState> THREAD_LOCAL_STATE = new ThreadLocal<>();

    /** Thread-local buffer for payload keys. */
    private static final ThreadLocal<ByteBuffer> PAYLOAD_KEY_BUFFER =
            ThreadLocal.withInitial(() -> allocate(PAYLOAD_KEY_SIZE).order(KEY_BYTE_ORDER));

    /** Table ID. */
    private final int tableId;

    /** Partition id. */
    private final int partitionId;

    /** Upper bound for scans. */
    private final Slice upperBound;

    /** Partition data column family. */
    final ColumnFamilyHandle partCf;

    final ColumnFamilyHandle dataCf;

    /** Read options for regular scans. */
    final ReadOptions upperBoundReadOpts;

    /** Read options for total order scans. */
    final ReadOptions scanReadOpts;

    final LockByRowId lockByRowId = new LockByRowId();

    /** Prefix for finding the beginning of the partition. */
    private final byte[] partitionStartPrefix;

    /** Prefix for finding the ending of the partition. */
    private final byte[] partitionEndPrefix;

    PartitionDataHelper(int tableId, int partitionId, ColumnFamilyHandle partCf, ColumnFamilyHandle dataCf) {
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.partCf = partCf;
        this.dataCf = dataCf;

        this.partitionStartPrefix = compositeKey(tableId, partitionId);
        this.partitionEndPrefix = incrementPrefix(partitionStartPrefix);

        this.upperBound = new Slice(partitionEndPrefix);
        this.upperBoundReadOpts = new ReadOptions().setIterateUpperBound(upperBound);
        this.scanReadOpts = new ReadOptions().setIterateUpperBound(upperBound).setAutoPrefixMode(true);
    }

    public int partitionId() {
        return partitionId;
    }

    /**
     * Creates a prefix of all keys in the given partition.
     */
    public byte[] partitionStartPrefix() {
        return partitionStartPrefix;
    }

    /**
     * Creates a prefix of all keys in the next partition, used as an exclusive bound.
     */
    public byte[] partitionEndPrefix() {
        return partitionEndPrefix;
    }

    void putCommittedDataIdKey(ByteBuffer buffer, RowId rowId, HybridTimestamp timestamp) {
        assert buffer.order() == KEY_BYTE_ORDER;
        assert rowId.partitionId() == partitionId : "rowPartitionId=" + rowId.partitionId() + ", storagePartitionId=" + partitionId;

        buffer.putInt(tableId);
        buffer.putShort((short) partitionId);
        putRowIdUuid(buffer, rowId.uuid());
        putTimestampDesc(buffer, timestamp);

        buffer.flip();
    }

    void putGcKey(ByteBuffer gcKeyBuffer, RowId rowId, HybridTimestamp timestamp) {
        assert gcKeyBuffer.order() == KEY_BYTE_ORDER;
        assert rowId.partitionId() == partitionId : "rowPartitionId=" + rowId.partitionId() + ", storagePartitionId=" + partitionId;

        gcKeyBuffer.putInt(tableId);
        gcKeyBuffer.putShort((short) partitionId);
        putTimestampNatural(gcKeyBuffer, timestamp);
        putRowIdUuid(gcKeyBuffer, rowId.uuid());

        gcKeyBuffer.flip();
    }

    void putRowId(ByteBuffer keyBuffer, RowId rowId) {
        assert rowId.partitionId() == partitionId : "rowPartitionId=" + rowId.partitionId() + ", storagePartitionId=" + partitionId;

        putRowIdUuid(keyBuffer, rowId.uuid());
    }

    RowId getRowId(ByteBuffer keyBuffer, int offset) {
        assert partitionId == (keyBuffer.getShort(TABLE_ID_SIZE) & 0xFFFF);

        return new RowId(partitionId, getRowIdUuid(keyBuffer, offset));
    }

    /**
     * Returns a WriteBatch that can be used by the affiliated storage implementation (like indices) to maintain consistency when run
     * inside the {@link MvPartitionStorage#runConsistently} method.
     */
    static @Nullable WriteBatchWithIndex currentWriteBatch() {
        ThreadLocalState state = THREAD_LOCAL_STATE.get();

        return state == null ? null : state.batch;
    }

    /**
     * Same as {@link #currentWriteBatch()}, with the exception that the resulting write batch is not expected to be null.
     */
    public static WriteBatchWithIndex requireWriteBatch() {
        ThreadLocalState state = THREAD_LOCAL_STATE.get();

        assert state != null : "Attempting to write data outside of data access closure.";

        return state.batch;
    }

    /**
     * Creates a byte array key, that consists of table or index ID (4 bytes), followed by a partition ID (2 bytes).
     */
    public static byte[] compositeKey(int tableOrIndexId, int partitionId) {
        return allocate(ROW_ID_OFFSET).order(KEY_BYTE_ORDER)
                .putInt(tableOrIndexId)
                .putShort((short) partitionId)
                .array();
    }

    /**
     * Writes a timestamp into a byte buffer, in descending lexicographical bytes order.
     */
    static void putTimestampDesc(ByteBuffer buf, HybridTimestamp ts) {
        assert buf.order() == KEY_BYTE_ORDER;

        // "bitwise negation" turns ascending order into a descending one.
        buf.putLong(~ts.longValue());
    }

    static HybridTimestamp readTimestampDesc(ByteBuffer keyBuf) {
        assert keyBuf.order() == KEY_BYTE_ORDER;

        long time = ~keyBuf.getLong(ROW_PREFIX_SIZE);

        return hybridTimestamp(time);
    }

    /**
     * Writes a timestamp into a byte buffer, in ascending lexicographical bytes order.
     */
    static void putTimestampNatural(ByteBuffer buf, HybridTimestamp ts) {
        assert buf.order() == KEY_BYTE_ORDER;

        buf.putLong(ts.longValue());
    }

    static HybridTimestamp readTimestampNatural(ByteBuffer keyBuf, int offset) {
        assert keyBuf.order() == KEY_BYTE_ORDER;

        long time = keyBuf.getLong(offset);

        return hybridTimestamp(time);
    }

    static RocksIterator wrapIterator(RocksIterator it, ColumnFamilyHandle cf) {
        return wrapIterator(it, currentWriteBatch(), cf);
    }

    static RocksIterator wrapIterator(RocksIterator it, @Nullable WriteBatchWithIndex writeBatch, ColumnFamilyHandle cf) {
        // "count()" check is mandatory. Write batch iterator without any updates just crashes everything.
        // It's not documented, but this is exactly how it should be used.
        if (writeBatch != null && writeBatch.count() > 0) {
            return writeBatch.newIteratorWithBase(cf, it);
        }

        return it;
    }

    static byte @Nullable [] getFromBatchAndDb(
            RocksDB db, ColumnFamilyHandle cfHandle, ReadOptions readOptions, byte[] key
    ) throws RocksDBException {
        return getFromBatchAndDb(db, currentWriteBatch(), cfHandle, readOptions, key);
    }

    static byte @Nullable [] getFromBatchAndDb(
            RocksDB db, @Nullable WriteBatchWithIndex writeBatch, ColumnFamilyHandle cfHandle, ReadOptions readOptions, byte[] key
    ) throws RocksDBException {
        return writeBatch == null || writeBatch.count() == 0
                ? db.get(cfHandle, readOptions, key)
                : writeBatch.getFromBatchAndDB(db, cfHandle, readOptions, key);
    }

    /**
     * Converts an internal serialized presentation of a binary row into its Java Object counterpart.
     */
    static BinaryRow deserializeRow(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);

        return deserializeRow(buffer);
    }

    /**
     * Converts an internal serialized presentation of a binary row into its Java Object counterpart.
     */
    static BinaryRow deserializeRow(ByteBuffer buffer) {
        assert buffer.order() == ByteOrder.BIG_ENDIAN;

        int schemaVersion = Short.toUnsignedInt(buffer.getShort());
        ByteBuffer binaryTupleSlice = buffer.slice().order(BinaryTuple.ORDER);

        return new BinaryRowImpl(schemaVersion, binaryTupleSlice);
    }

    byte[] createPayloadKey(ByteBuffer dataId) {
        byte[] result = PAYLOAD_KEY_BUFFER.get()
                .clear()
                .putInt(tableId)
                .putShort((short) partitionId)
                .put(dataId)
                .array();

        dataId.rewind();

        // Always use 0 for the first bit (tombstone flag), because it only makes sense when data ID is stored as a value.
        setFirstBit(result, result.length - 1, false);

        return result;
    }

    /**
     * Changes the first bit of the byte identified by an index in an array.
     *
     * @param array Array containing the byte to change.
     * @param index Index of the byte inside the array.
     * @param value If {@code true} - sets the bit to 1, else to 0.
     */
    static void setFirstBit(byte[] array, int index, boolean value) {
        if (value) {
            array[index] |= 0x01;
        } else {
            array[index] &= 0xFE;
        }
    }

    /**
     * Returns {@code true} if the given data ID points to a tombstone.
     */
    static boolean isTombstone(ByteBuffer dataId) {
        byte lastByte = dataId.get(dataId.limit() - 1);

        return (lastByte & 0x1) != 0;
    }

    /**
     * Returns {@code true} if the given data ID points to a tombstone.
     */
    static boolean isTombstone(byte[] dataId) {
        byte lastByte = dataId[dataId.length - 1];

        return (lastByte & 0x1) != 0;
    }

    @Override
    public void close() {
        RocksUtils.closeAll(scanReadOpts, upperBoundReadOpts, upperBound);
    }
}
