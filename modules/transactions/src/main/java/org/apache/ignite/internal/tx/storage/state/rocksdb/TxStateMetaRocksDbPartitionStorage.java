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

package org.apache.ignite.internal.tx.storage.state.rocksdb;

import static org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbStorage.TABLE_OR_ZONE_PREFIX_SIZE_BYTES;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.storage.lease.LeaseInfoSerializer;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

/**
 * A wrapper around a RocksDB column family to store TX storage meta information.
 */
class TxStateMetaRocksDbPartitionStorage {
    /** Key length for the payload. Consists of a 1-byte prefix, tableId (4 bytes) and partitionId (2 bytes), in Big Endian. */
    private static final int KEY_SIZE_BYTES = TABLE_OR_ZONE_PREFIX_SIZE_BYTES + Short.BYTES + 1;

    /**
     * Prefix to store meta information, such as last applied index and term.
     */
    private static final byte LAST_APPLIED_PREFIX = 0;

    /**
     * Prefix to last committed replication group configuration.
     */
    private static final byte CONF_PREFIX = 1;

    /**
     * Prefix for keys corresponding to the lease information.
     */
    private static final byte LEASE_INFO_PREFIX = 2;

    /**
     * Prefix for keys corresponding to the last saved snapshot information.
     */
    private static final byte SNAPSHOT_INFO_PREFIX = 3;

    private final ColumnFamily columnFamily;

    private final int tableId;

    private final int partitionId;

    private final byte[] lastAppliedKey;

    private final byte[] confKey;

    private final byte[] leaseInfoKey;

    private final byte[] snapshotInfoKey;

    private volatile long lastAppliedIndex;

    private volatile long lastAppliedTerm;

    private volatile byte @Nullable [] config;

    @Nullable
    private volatile LeaseInfo leaseInfo;

    TxStateMetaRocksDbPartitionStorage(ColumnFamily columnFamily, int tableId, int partitionId) {
        this.columnFamily = columnFamily;
        this.partitionId = partitionId;
        this.tableId = tableId;

        lastAppliedKey = createKey(LAST_APPLIED_PREFIX);
        confKey = createKey(CONF_PREFIX);
        leaseInfoKey = createKey(LEASE_INFO_PREFIX);
        snapshotInfoKey = createKey(SNAPSHOT_INFO_PREFIX);
    }

    private byte[] createKey(byte prefix) {
        return ByteBuffer.allocate(KEY_SIZE_BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .put(prefix)
                .putInt(tableId)
                .putShort((short) partitionId)
                .array();
    }

    void start() throws RocksDBException {
        byte[] lastAppliedBytes = columnFamily.get(lastAppliedKey);

        if (lastAppliedBytes != null) {
            ByteBuffer buf = ByteBuffer.wrap(lastAppliedBytes).order(ByteOrder.BIG_ENDIAN);

            lastAppliedIndex = buf.getLong();
            lastAppliedTerm = buf.getLong();
        }

        config = columnFamily.get(confKey);

        byte[] leaseBytes = columnFamily.get(leaseInfoKey);

        if (leaseBytes != null) {
            leaseInfo = VersionedSerialization.fromBytes(leaseBytes, LeaseInfoSerializer.INSTANCE);
        }
    }

    /**
     * Special method to be used with TX storages using a legacy format. Such storages saved last applied index and term inside a
     * different column family.
     */
    // TODO: remove this method after the colocation track migration, see https://issues.apache.org/jira/browse/IGNITE-22522.
    void startInCompatibilityMode(long lastAppliedIndex, long lastAppliedTerm) throws RocksDBException {
        byte[] lastAppliedBytes = columnFamily.get(lastAppliedKey);

        if (lastAppliedBytes != null) {
            ByteBuffer buf = ByteBuffer.wrap(lastAppliedBytes).order(ByteOrder.BIG_ENDIAN);

            this.lastAppliedIndex = buf.getLong();
            this.lastAppliedTerm = buf.getLong();
        } else {
            this.lastAppliedIndex = lastAppliedIndex;
            this.lastAppliedTerm = lastAppliedTerm;
        }

        config = columnFamily.get(confKey);

        byte[] leaseBytes = columnFamily.get(leaseInfoKey);

        if (leaseBytes != null) {
            leaseInfo = VersionedSerialization.fromBytes(leaseBytes, LeaseInfoSerializer.INSTANCE);
        }
    }

    long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    long lastAppliedTerm() {
        return lastAppliedTerm;
    }

    byte @Nullable [] configuration() {
        return config;
    }

    @Nullable LeaseInfo leaseInfo() {
        return leaseInfo;
    }

    byte @Nullable [] snapshotInfo() throws RocksDBException {
        return columnFamily.get(snapshotInfoKey);
    }

    void updateLastApplied(WriteBatch writeBatch, long index, long term) throws RocksDBException {
        columnFamily.put(writeBatch, lastAppliedKey, indexAndTermToBytes(index, term));

        lastAppliedIndex = index;
        lastAppliedTerm = term;
    }

    void updateConfiguration(WriteBatch writeBatch, byte[] config) throws RocksDBException {
        columnFamily.put(writeBatch, confKey, config);

        this.config = config;
    }

    void updateLease(WriteBatch writeBatch, LeaseInfo leaseInfo) throws RocksDBException {
        columnFamily.put(writeBatch, leaseInfoKey, VersionedSerialization.toBytes(leaseInfo, LeaseInfoSerializer.INSTANCE));

        this.leaseInfo = leaseInfo;
    }

    void updateSnapshotInfo(WriteBatch writeBatch, byte[] snapshotInfo) throws RocksDBException {
        columnFamily.put(writeBatch, snapshotInfoKey, snapshotInfo);
    }

    private static byte[] indexAndTermToBytes(long lastAppliedIndex, long lastAppliedTerm) {
        return ByteBuffer.allocate(2 * Long.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putLong(lastAppliedIndex)
                .putLong(lastAppliedTerm)
                .array();
    }

    void clear(WriteBatch writeBatch) throws RocksDBException {
        columnFamily.delete(writeBatch, lastAppliedKey);
        columnFamily.delete(writeBatch, confKey);
        columnFamily.delete(writeBatch, leaseInfoKey);
        columnFamily.delete(writeBatch, snapshotInfoKey);

        lastAppliedIndex = 0;
        lastAppliedTerm = 0;
        config = null;
        leaseInfo = null;
    }
}
