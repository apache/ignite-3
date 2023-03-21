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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.KEY_BYTE_ORDER;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.PARTITION_ID_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.UUID_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.putUuid;
import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.readUuid;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;

/**
 * Wrapper around the "meta" Column Family inside a RocksDB-based storage, which stores some auxiliary information needed for internal
 * storage logic.
 */
public class RocksDbMetaStorage {
    /** Name of the key that corresponds to a list of existing partition IDs of a storage. */
    private static final byte[] PARTITION_ID_PREFIX = "part".getBytes(UTF_8);

    /** Name of the key that is out of range of the partition ID key prefix, used as an exclusive bound. */
    private static final byte[] PARTITION_ID_PREFIX_END = RocksUtils.incrementPrefix(PARTITION_ID_PREFIX);

    /** Index meta key prefix. */
    private static final byte[] INDEX_META_KEY_PREFIX = "index-meta".getBytes(UTF_8);

    /** Index meta key size in bytes. */
    private static final int INDEX_META_KEY_SIZE = INDEX_META_KEY_PREFIX.length + PARTITION_ID_SIZE + UUID_SIZE;

    private final ColumnFamily metaColumnFamily;

    RocksDbMetaStorage(ColumnFamily metaColumnFamily) {
        this.metaColumnFamily = metaColumnFamily;
    }

    /**
     * Returns a column family instance, associated with the meta storage.
     */
    ColumnFamily columnFamily() {
        return metaColumnFamily;
    }

    /**
     * Returns a list of partition IDs that exist in the associated storage.
     *
     * @return list of partition IDs
     */
    int[] getPartitionIds() {
        Stream.Builder<byte[]> data = Stream.builder();

        try (
                var upperBound = new Slice(PARTITION_ID_PREFIX_END);
                var options = new ReadOptions().setIterateUpperBound(upperBound);
                RocksIterator it = metaColumnFamily.newIterator(options);
        ) {
            it.seek(PARTITION_ID_PREFIX);

            RocksUtils.forEach(it, (key, value) -> data.add(key));
        } catch (RocksDBException e) {
            throw new StorageException("Error when reading a list of partition IDs from the meta Column Family", e);
        }

        return data.build()
                .mapToInt(bytes -> {
                    // Bytes are stored in BE order
                    int higherByte = bytes[PARTITION_ID_PREFIX.length] & 0xFF;
                    int lowerByte = bytes[PARTITION_ID_PREFIX.length + 1] & 0xFF;

                    return (higherByte << 8) | lowerByte;
                })
                .toArray();
    }

    /**
     * Saves the given partition ID into the meta Column Family.
     *
     * @param partitionId partition ID
     */
    void putPartitionId(int partitionId) {
        try {
            metaColumnFamily.put(partitionIdKey(partitionId), BYTE_EMPTY_ARRAY);
        } catch (RocksDBException e) {
            throw new StorageException("Unable to save partition " + partitionId + " in the meta Column Family", e);
        }
    }

    /**
     * Puts last row ID for which the index was built, {@code null} means index building is finished.
     *
     * @param partitionId Partition ID.
     * @param indexId Index ID.
     * @param rowId Row ID.
     */
    public void putIndexLastBuildRowId(int partitionId, UUID indexId, @Nullable RowId rowId) {
        try {
            metaColumnFamily.put(indexMetaKey(partitionId, indexId), indexLastBuildRowId(rowId));
        } catch (RocksDBException e) {
            throw new StorageException(
                    "Failed to save last row ID for which the index was built: [partitionId={}, indexId={}, rowId={}]",
                    e,
                    partitionId, indexId, rowId
            );
        }
    }

    /**
     * Reads last row ID for which the index was built, {@code null} means index building is finished.
     *
     * @param partitionId Partition ID.
     * @param indexId Index ID.
     * @param ifAbsent Will be returned if last row ID for which the index was built has never been saved.
     */
    public @Nullable RowId readIndexLastBuildRowId(int partitionId, UUID indexId, RowId ifAbsent) {
        try {
            byte[] lastBuildRowIdBytes = metaColumnFamily.get(indexMetaKey(partitionId, indexId));

            if (lastBuildRowIdBytes == null) {
                return ifAbsent;
            }

            if (lastBuildRowIdBytes.length == 0) {
                return null;
            }

            return new RowId(partitionId, readUuid(ByteBuffer.wrap(lastBuildRowIdBytes), 0));
        } catch (RocksDBException e) {
            throw new StorageException(
                    "Failed to read last row ID for which the index was built: [partitionId={}, indexId={}]",
                    e,
                    partitionId, indexId
            );
        }
    }

    static byte[] partitionIdKey(int partitionId) {
        assert partitionId >= 0 && partitionId <= 0xFFFF : partitionId;

        return ByteBuffer.allocate(PARTITION_ID_PREFIX.length + PARTITION_ID_SIZE)
                .order(KEY_BYTE_ORDER)
                .put(PARTITION_ID_PREFIX)
                .putShort((short) partitionId)
                .array();
    }

    static byte[] indexMetaKey(int partitionId, UUID indexId) {
        assert partitionId >= 0 && partitionId <= 0xFFFF : partitionId;

        ByteBuffer buffer = ByteBuffer.allocate(INDEX_META_KEY_SIZE).order(KEY_BYTE_ORDER);

        buffer.put(INDEX_META_KEY_PREFIX).putShort((short) partitionId);

        putUuid(buffer, indexId);

        return buffer.array();
    }

    static byte[] indexLastBuildRowId(@Nullable RowId rowId) {
        if (rowId == null) {
            return BYTE_EMPTY_ARRAY;
        }

        ByteBuffer buffer = ByteBuffer.allocate(UUID_SIZE).order(KEY_BYTE_ORDER);

        putUuid(buffer, rowId.uuid());

        return buffer.array();
    }
}
