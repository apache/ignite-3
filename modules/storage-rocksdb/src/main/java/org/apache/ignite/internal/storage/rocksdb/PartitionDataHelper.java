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
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.KEY_BYTE_ORDER;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.PARTITION_ID_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.ROW_ID_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.getRowIdUuid;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.putRowIdUuid;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.util.ReentrantLockByRowId;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.Slice;

/** Helper for the partition data. */
class PartitionDataHelper implements ManuallyCloseable {
    /** Position of row id inside the key. */
    static final int ROW_ID_OFFSET = Short.BYTES;

    /** Size of the key without timestamp. */
    static final int ROW_PREFIX_SIZE = ROW_ID_OFFSET + ROW_ID_SIZE;

    /** Maximum size of the data key. */
    static final int MAX_KEY_SIZE = ROW_PREFIX_SIZE + HYBRID_TIMESTAMP_SIZE;

    /** Transaction id size (part of the transaction state). */
    private static final int TX_ID_SIZE = 2 * Long.BYTES;

    /** Commit table id size (part of the transaction state). */
    private static final int TABLE_ID_SIZE = 2 * Long.BYTES;

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

    static final ByteOrder TABLE_ROW_BYTE_ORDER = ByteBufferRow.ORDER;

    /** Thread-local direct buffer instance to read keys from RocksDB. */
    static final ThreadLocal<ByteBuffer> MV_KEY_BUFFER = withInitial(() -> allocateDirect(MAX_KEY_SIZE).order(KEY_BYTE_ORDER));

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

    final ReentrantLockByRowId lockByRowId = new ReentrantLockByRowId();

    PartitionDataHelper(int partitionId, ColumnFamilyHandle partCf) {
        this.partitionId = partitionId;
        this.partCf = partCf;

        this.upperBound = new Slice(partitionEndPrefix());
        this.upperBoundReadOpts = new ReadOptions().setIterateUpperBound(upperBound);
        this.scanReadOpts = new ReadOptions().setIterateUpperBound(upperBound).setTotalOrderSeek(true);
    }

    /**
     * Creates a prefix of all keys in the given partition.
     */
    byte[] partitionStartPrefix() {
        return unsignedShortAsBytes(partitionId);
    }

    /**
     * Creates a prefix of all keys in the next partition, used as an exclusive bound.
     */
    byte[] partitionEndPrefix() {
        return unsignedShortAsBytes(partitionId + 1);
    }

    void putDataKey(ByteBuffer dataKeyBuffer, RowId rowId, HybridTimestamp timestamp) {
        assert rowId.partitionId() == partitionId : "rowPartitionId=" + rowId.partitionId() + ", storagePartitionId=" + partitionId;

        dataKeyBuffer.putShort((short) partitionId);
        putRowIdUuid(dataKeyBuffer, rowId.uuid());
        putTimestampDesc(dataKeyBuffer, timestamp);

        dataKeyBuffer.flip();
    }

    void putGcKey(ByteBuffer gcKeyBuffer, RowId rowId, HybridTimestamp timestamp) {
        assert rowId.partitionId() == partitionId : "rowPartitionId=" + rowId.partitionId() + ", storagePartitionId=" + partitionId;

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
        assert partitionId == (keyBuffer.getShort(0) & 0xFFFF);

        return new RowId(partitionId, getRowIdUuid(keyBuffer, offset));
    }

    /**
     * Stores unsigned short (represented by int) in the byte array.
     *
     * @param value Unsigned short value.
     * @return Byte array with unsigned short.
     */
    private static byte[] unsignedShortAsBytes(int value) {
        return new byte[] {(byte) (value >>> 8), (byte) value};
    }

    /**
     * Writes a timestamp into a byte buffer, in descending lexicographical bytes order.
     */
    static void putTimestampDesc(ByteBuffer buf, HybridTimestamp ts) {
        assert buf.order() == KEY_BYTE_ORDER;

        // "bitwise negation" turns ascending order into a descending one.
        buf.putLong(~ts.getPhysical());
        buf.putInt(~ts.getLogical());
    }

    static HybridTimestamp readTimestampDesc(ByteBuffer keyBuf) {
        assert keyBuf.order() == KEY_BYTE_ORDER;

        long physical = ~keyBuf.getLong(ROW_PREFIX_SIZE);
        int logical = ~keyBuf.getInt(ROW_PREFIX_SIZE + Long.BYTES);

        return new HybridTimestamp(physical, logical);
    }

    /**
     * Writes a timestamp into a byte buffer, in ascending lexicographical bytes order.
     */
    static void putTimestampNatural(ByteBuffer buf, HybridTimestamp ts) {
        assert buf.order() == KEY_BYTE_ORDER;

        buf.putLong(ts.getPhysical());
        buf.putInt(ts.getLogical());
    }

    static HybridTimestamp readTimestampNatural(ByteBuffer keyBuf, int offset) {
        assert keyBuf.order() == KEY_BYTE_ORDER;

        long physical = keyBuf.getLong(offset);
        int logical = keyBuf.getInt(offset + Long.BYTES);

        return new HybridTimestamp(physical, logical);
    }

    @Override
    public void close() {
        RocksUtils.closeAll(upperBoundReadOpts, upperBound);
    }
}
