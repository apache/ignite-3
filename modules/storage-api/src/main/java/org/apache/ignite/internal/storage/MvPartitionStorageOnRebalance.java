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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Decorator for {@link MvPartitionStorage} on full rebalance.
 *
 * <p>Allows you to use methods to change data, throws {@link IllegalStateException} when reading data.
 */
public class MvPartitionStorageOnRebalance implements MvPartitionStorage {
    private static final String ERROR_MESSAGE = "Multi-versioned partition storage is in full rebalancing, data reading is not available.";

    private final MvPartitionStorage delegate;

    /**
     * Constructor.
     *
     * @param delegate Delegate.
     */
    public MvPartitionStorageOnRebalance(MvPartitionStorage delegate) {
        this.delegate = delegate;
    }

    @Override
    public <V> V runConsistently(WriteClosure<V> closure) throws StorageException {
        return delegate.runConsistently(closure);
    }

    @Override
    public CompletableFuture<Void> flush() {
        return delegate.flush();
    }

    @Override
    public long lastAppliedIndex() {
        return delegate.lastAppliedIndex();
    }

    @Override
    public void lastApplied(long lastAppliedIndex, long lastAppliedTerm) throws StorageException {
        delegate.lastApplied(lastAppliedIndex, lastAppliedTerm);
    }

    @Override
    public long lastAppliedTerm() {
        return delegate.lastAppliedTerm();
    }

    @Override
    public @Nullable RaftGroupConfiguration committedGroupConfiguration() {
        return delegate.committedGroupConfiguration();
    }

    @Override
    public void committedGroupConfiguration(RaftGroupConfiguration config) {
        delegate.committedGroupConfiguration(config);
    }

    @Override
    public long persistedIndex() {
        return delegate.persistedIndex();
    }

    @Override
    public ReadResult read(RowId rowId, HybridTimestamp timestamp) throws StorageException {
        throw createDataWriteOnlyException();
    }

    @Override
    public @Nullable BinaryRow addWrite(RowId rowId, @Nullable BinaryRow row, UUID txId, UUID commitTableId, int commitPartitionId)
            throws TxIdMismatchException, StorageException {
        return delegate.addWrite(rowId, row, txId, commitTableId, commitPartitionId);
    }

    @Override
    public @Nullable BinaryRow abortWrite(RowId rowId) throws StorageException {
        return delegate.abortWrite(rowId);
    }

    @Override
    public void commitWrite(RowId rowId, HybridTimestamp timestamp) throws StorageException {
        delegate.commitWrite(rowId, timestamp);
    }

    @Override
    public void addWriteCommitted(RowId rowId, BinaryRow row, HybridTimestamp commitTimestamp) throws StorageException {
        delegate.addWriteCommitted(rowId, row, commitTimestamp);
    }

    @Override
    public Cursor<ReadResult> scanVersions(RowId rowId) throws StorageException {
        throw createDataWriteOnlyException();
    }

    @Override
    public PartitionTimestampCursor scan(HybridTimestamp timestamp) throws StorageException {
        throw createDataWriteOnlyException();
    }

    @Override
    public @Nullable RowId closestRowId(RowId lowerBound) throws StorageException {
        throw createDataWriteOnlyException();
    }

    @Override
    public long rowsCount() throws StorageException {
        return delegate.rowsCount();
    }

    @Override
    public void close() {
        delegate.close();
    }

    private IllegalStateException createDataWriteOnlyException() {
        return new IllegalStateException(ERROR_MESSAGE);
    }
}
