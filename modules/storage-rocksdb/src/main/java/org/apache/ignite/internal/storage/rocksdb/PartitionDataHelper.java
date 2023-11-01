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
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatchWithIndex;

/** Helper for the partition data. */
public final class PartitionDataHelper implements ManuallyCloseable {
    /** Position of row id inside the key. */
    static final int ROW_ID_OFFSET = TABLE_ID_SIZE + Short.BYTES;

    /** Size of the key without timestamp. */
    public static final int ROW_PREFIX_SIZE = ROW_ID_OFFSET + ROW_ID_SIZE;

    /** Maximum size of the data key. */
    static final int MAX_KEY_SIZE = ROW_PREFIX_SIZE + HYBRID_TIMESTAMP_SIZE;

    /** Transaction id size (part of the transaction state). */
    private static final int TX_ID_SIZE = 2 * Long.BYTES;

    /** Size of the value header (transaction state). */
    static final int VALUE_HEADER_SIZE = TX_ID_SIZE + TABLE_ID_SIZE + PARTITION_ID_SIZE;

    /** Transaction id offset. */
    static final int TX_ID_OFFSET = 0;

    /** Commit table id offset. */
    static final int TABLE_ID_OFFSET = TX_ID_SIZE;

    /** Commit partition id offset. */
    static final int PARTITION_ID_OFFSET = TABLE_ID_OFFSET + TABLE_ID_SIZE;

    /** Value offset (if transaction state is present). */
    static final int VALUE_OFFSET = VALUE_HEADER_SIZE;

    /** Thread-local direct buffer instance to read keys from RocksDB. */
    static final ThreadLocal<ByteBuffer> MV_KEY_BUFFER = withInitial(() -> allocateDirect(MAX_KEY_SIZE).order(KEY_BYTE_ORDER));

    /** Thread-local write batch for {@link MvPartitionStorage#runConsistently(WriteClosure)}. */
    static final ThreadLocal<ThreadLocalState> THREAD_LOCAL_STATE = new ThreadLocal<>();

    /** Table ID. */
    private final int tableId;

    /** Partition id. */
    private final int partitionId;

    /** Upper bound for scans. */
    private final Slice upperBound;

    /** Partition data column family. */
    final ColumnFamilyHandle partCf;

    /** Read options for regular scans. */
    final ReadOptions upperBoundReadOpts;

    /** Read options for total order scans. */
    final ReadOptions scanReadOpts;

    final LockByRowId lockByRowId = new LockByRowId();

    /** Prefix for finding the beginning of the partition. */
    private final byte[] partitionStartPrefix;

    /** Prefix for finding the ending of the partition. */
    private final byte[] partitionEndPrefix;

    PartitionDataHelper(int tableId, int partitionId, ColumnFamilyHandle partCf) {
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.partCf = partCf;

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

    void putDataKey(ByteBuffer dataKeyBuffer, RowId rowId, HybridTimestamp timestamp) {
        assert rowId.partitionId() == partitionId : "rowPartitionId=" + rowId.partitionId() + ", storagePartitionId=" + partitionId;

        dataKeyBuffer.putInt(tableId);
        dataKeyBuffer.putShort((short) partitionId);
        putRowIdUuid(dataKeyBuffer, rowId.uuid());
        putTimestampDesc(dataKeyBuffer, timestamp);

        dataKeyBuffer.flip();
    }

    void putGcKey(ByteBuffer gcKeyBuffer, RowId rowId, HybridTimestamp timestamp) {
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
        return ByteBuffer.allocate(ROW_ID_OFFSET).order(KEY_BYTE_ORDER)
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

    RocksIterator wrapIterator(RocksIterator it, ColumnFamilyHandle cf) {
        WriteBatchWithIndex writeBatch = currentWriteBatch();

        if (writeBatch != null && writeBatch.count() > 0) {
            return writeBatch.newIteratorWithBase(cf, it);
        }

        return it;
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

    @Override
    public void close() {
        RocksUtils.closeAll(scanReadOpts, upperBoundReadOpts, upperBound);
    }
}
