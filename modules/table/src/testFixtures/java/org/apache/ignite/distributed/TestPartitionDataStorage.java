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

package org.apache.ignite.distributed;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.storage.gc.GcEntry;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.table.distributed.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.table.distributed.raft.RaftGroupConfigurationConverter;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Test implementation of {@link PartitionDataStorage}.
 */
public class TestPartitionDataStorage implements PartitionDataStorage {
    private final int tableId;

    private final int partitionId;

    private final MvPartitionStorage partitionStorage;

    private final RaftGroupConfigurationConverter configurationConverter = new RaftGroupConfigurationConverter();

    /** Constructor. */
    public TestPartitionDataStorage(
            int tableId,
            int partitionId,
            MvPartitionStorage partitionStorage
    ) {
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.partitionStorage = partitionStorage;
    }

    @Override
    public int tableId() {
        return tableId;
    }

    @Override
    public int partitionId() {
        return partitionId;
    }

    @Override
    public <V> V runConsistently(WriteClosure<V> closure) throws StorageException {
        return partitionStorage.runConsistently(closure);
    }

    @Override
    public void acquirePartitionSnapshotsReadLock() {
        // There is no 'write' side, so we don't need to take any lock.
    }

    @Override
    public void releasePartitionSnapshotsReadLock() {
        // There is no 'write' side, so we don't need to releasetestbala any lock.
    }

    @Override
    public CompletableFuture<Void> flush() {
        return partitionStorage.flush();
    }

    @Override
    public long lastAppliedIndex() {
        return partitionStorage.lastAppliedIndex();
    }

    @Override
    public long lastAppliedTerm() {
        return partitionStorage.lastAppliedTerm();
    }

    @Override
    public void lastApplied(long lastAppliedIndex, long lastAppliedTerm) throws StorageException {
        partitionStorage.lastApplied(lastAppliedIndex, lastAppliedTerm);
    }

    @Override
    public void committedGroupConfiguration(RaftGroupConfiguration config) {
        partitionStorage.committedGroupConfiguration(configurationConverter.toBytes(config));
    }

    @Override
    public @Nullable BinaryRow addWrite(RowId rowId, @Nullable BinaryRow row, UUID txId, int commitTableId,
            int commitPartitionId) throws TxIdMismatchException, StorageException {
        return partitionStorage.addWrite(rowId, row, txId, commitTableId, commitPartitionId);
    }

    @Override
    public void addWriteCommitted(RowId rowId, @Nullable BinaryRow row, HybridTimestamp commitTs) {
        partitionStorage.addWriteCommitted(rowId, row, commitTs);
    }

    @Override
    public @Nullable BinaryRow abortWrite(RowId rowId) throws StorageException {
        return partitionStorage.abortWrite(rowId);
    }

    @Override
    public void commitWrite(RowId rowId, HybridTimestamp timestamp) throws StorageException {
        partitionStorage.commitWrite(rowId, timestamp);
    }

    @Override
    public Cursor<ReadResult> scanVersions(RowId rowId) throws StorageException {
        return partitionStorage.scanVersions(rowId);
    }

    @Override
    public MvPartitionStorage getStorage() {
        return partitionStorage;
    }

    @Override
    public void close() {
    }

    @Override
    public PartitionTimestampCursor scan(HybridTimestamp timestamp) throws StorageException {
        return partitionStorage.scan(timestamp);
    }

    @Override
    public @Nullable GcEntry peek(HybridTimestamp lowWatermark) {
        return partitionStorage.peek(lowWatermark);
    }

    @Override
    public @Nullable BinaryRow vacuum(GcEntry entry) {
        return partitionStorage.vacuum(entry);
    }

    @Override
    public void updateLease(long leaseStartTime, String primaryReplicaNodeId) {
        partitionStorage.updateLease(leaseStartTime, primaryReplicaNodeId);
    }

    @Override
    public long leaseStartTime() {
        return partitionStorage.leaseStartTime();
    }

    @Override
    public String primaryReplicaNodeId() {
        return partitionStorage.primaryReplicaNodeId();
    }
}
