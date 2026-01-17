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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionDataStorage;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.RaftGroupConfigurationConverter;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.AbortResult;
import org.apache.ignite.internal.storage.AddWriteCommittedResult;
import org.apache.ignite.internal.storage.AddWriteResult;
import org.apache.ignite.internal.storage.CommitResult;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.gc.GcEntry;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
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
    public CompletableFuture<Void> flush(boolean trigger) {
        return partitionStorage.flush(trigger);
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
    public @Nullable RaftGroupConfiguration committedGroupConfiguration() {
        return configurationConverter.fromBytes(partitionStorage.committedGroupConfiguration());
    }

    @Override
    public AddWriteResult addWrite(
            RowId rowId,
            @Nullable BinaryRow row,
            UUID txId,
            int commitTableOrZoneId,
            int commitPartitionId
    ) throws StorageException {
        return partitionStorage.addWrite(rowId, row, txId, commitTableOrZoneId, commitPartitionId);
    }

    @Override
    public AddWriteCommittedResult addWriteCommitted(
            RowId rowId,
            @Nullable BinaryRow row,
            HybridTimestamp commitTimestamp
    ) throws StorageException {
        return partitionStorage.addWriteCommitted(rowId, row, commitTimestamp);
    }

    @Override
    public AbortResult abortWrite(RowId rowId, UUID txId) throws StorageException {
        return partitionStorage.abortWrite(rowId, txId);
    }

    @Override
    public CommitResult commitWrite(RowId rowId, HybridTimestamp timestamp, UUID txId) throws StorageException {
        return partitionStorage.commitWrite(rowId, timestamp, txId);
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
    public List<GcEntry> peek(HybridTimestamp lowWatermark, int count) {
        return partitionStorage.peek(lowWatermark, count);
    }

    @Override
    public @Nullable BinaryRow vacuum(GcEntry entry) {
        return partitionStorage.vacuum(entry);
    }

    @Override
    public void updateLease(LeaseInfo leaseInfo) {
        partitionStorage.updateLease(leaseInfo);
    }

    @Override
    public @Nullable LeaseInfo leaseInfo() {
        return partitionStorage.leaseInfo();
    }
}
