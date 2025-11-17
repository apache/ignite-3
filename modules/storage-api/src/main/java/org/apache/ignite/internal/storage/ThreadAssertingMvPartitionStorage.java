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

import static org.apache.ignite.internal.worker.ThreadAssertions.assertThreadAllowsToRead;
import static org.apache.ignite.internal.worker.ThreadAssertions.assertThreadAllowsToWrite;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.gc.GcEntry;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.worker.ThreadAssertingCursor;
import org.apache.ignite.internal.worker.ThreadAssertions;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.jetbrains.annotations.Nullable;

/**
 * {@link MvPartitionStorage} that performs thread assertions when doing read/write operations.
 *
 * @see ThreadAssertions
 */
public class ThreadAssertingMvPartitionStorage implements MvPartitionStorage, Wrapper {
    private final MvPartitionStorage partitionStorage;

    /** Constructor. */
    public ThreadAssertingMvPartitionStorage(MvPartitionStorage partitionStorage) {
        this.partitionStorage = partitionStorage;
    }

    @Override
    public <V> V runConsistently(WriteClosure<V> closure) throws StorageException {
        return partitionStorage.runConsistently(closure);
    }

    @Override
    public CompletableFuture<Void> flush(boolean trigger) {
        assertThreadAllowsToWrite();

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
        assertThreadAllowsToWrite();

        partitionStorage.lastApplied(lastAppliedIndex, lastAppliedTerm);
    }

    @Override
    public byte @Nullable [] committedGroupConfiguration() {
        return partitionStorage.committedGroupConfiguration();
    }

    @Override
    public void committedGroupConfiguration(byte[] config) {
        partitionStorage.committedGroupConfiguration(config);
    }

    @Override
    public ReadResult read(RowId rowId, HybridTimestamp timestamp) throws StorageException {
        assertThreadAllowsToRead();

        return partitionStorage.read(rowId, timestamp);
    }

    @Override
    public AddWriteResult addWrite(
            RowId rowId,
            @Nullable BinaryRow row,
            UUID txId,
            int commitTableOrZoneId,
            int commitPartitionId
    ) throws StorageException {
        assertThreadAllowsToWrite();

        return partitionStorage.addWrite(rowId, row, txId, commitTableOrZoneId, commitPartitionId);
    }

    @Override
    public AbortResult abortWrite(RowId rowId, UUID txId) throws StorageException {
        assertThreadAllowsToWrite();

        return partitionStorage.abortWrite(rowId, txId);
    }

    @Override
    public CommitResult commitWrite(RowId rowId, HybridTimestamp timestamp, UUID txId) throws StorageException {
        assertThreadAllowsToWrite();

        return partitionStorage.commitWrite(rowId, timestamp, txId);
    }

    @Override
    public AddWriteCommittedResult addWriteCommitted(
            RowId rowId,
            @Nullable BinaryRow row,
            HybridTimestamp commitTimestamp
    ) throws StorageException {
        assertThreadAllowsToWrite();

        return partitionStorage.addWriteCommitted(rowId, row, commitTimestamp);
    }

    @Override
    public Cursor<ReadResult> scanVersions(RowId rowId) throws StorageException {
        assertThreadAllowsToRead();

        return new ThreadAssertingCursor<>(partitionStorage.scanVersions(rowId));
    }

    @Override
    public PartitionTimestampCursor scan(HybridTimestamp timestamp) throws StorageException {
        assertThreadAllowsToRead();

        return new ThreadAssertingPartitionTimestampCursor(partitionStorage.scan(timestamp));
    }

    @Override
    public @Nullable RowId closestRowId(RowId lowerBound) throws StorageException {
        assertThreadAllowsToRead();

        return partitionStorage.closestRowId(lowerBound);
    }

    @Override
    public @Nullable RowId highestRowId() throws StorageException {
        assertThreadAllowsToRead();

        return partitionStorage.highestRowId();
    }

    @Override
    public List<RowMeta> rowsStartingWith(RowId lowerBoundInclusive, RowId upperBoundInclusive, int limit) throws StorageException {
        assertThreadAllowsToRead();

        return partitionStorage.rowsStartingWith(lowerBoundInclusive, upperBoundInclusive, limit);
    }

    @Override
    public @Nullable GcEntry peek(HybridTimestamp lowWatermark) {
        assertThreadAllowsToRead();

        return partitionStorage.peek(lowWatermark);
    }

    @Override
    public @Nullable BinaryRow vacuum(GcEntry entry) {
        assertThreadAllowsToWrite();

        return partitionStorage.vacuum(entry);
    }

    @Override
    public void updateLease(LeaseInfo leaseInfo) {
        assertThreadAllowsToWrite();

        partitionStorage.updateLease(leaseInfo);
    }

    @Override
    public @Nullable LeaseInfo leaseInfo() {
        return partitionStorage.leaseInfo();
    }

    @Override
    public long estimatedSize() {
        return partitionStorage.estimatedSize();
    }

    @Override
    public void close() {
        partitionStorage.close();
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return classToUnwrap.cast(partitionStorage);
    }
}
