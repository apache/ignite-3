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
import java.util.function.BiConsumer;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link MvPartitionStorage} that delegates all its method calls to another MvPartitionStorage instance.
 * It is intended to be a base for MvPartitionStorage decorators, that is, implementations that need to wrap around
 * an MvPartitionStorage changing its behavior in only a few methods.
 */
public abstract class DelegatingMvPartitionStorage implements MvPartitionStorage {
    private final MvPartitionStorage delegate;

    protected DelegatingMvPartitionStorage(MvPartitionStorage delegate) {
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
    public void lastAppliedIndex(long lastAppliedIndex) throws StorageException {
        delegate.lastAppliedIndex(lastAppliedIndex);
    }

    @Override
    public long persistedIndex() {
        return delegate.persistedIndex();
    }

    @Override
    public ReadResult read(RowId rowId, HybridTimestamp timestamp) throws StorageException {
        return delegate.read(rowId, timestamp);
    }

    @Override
    public @Nullable BinaryRow addWrite(RowId rowId, @Nullable BinaryRow row, UUID txId, UUID commitTableId,
            int commitPartitionId) throws TxIdMismatchException, StorageException {
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
        return delegate.scanVersions(rowId);
    }

    @Override
    public PartitionTimestampCursor scan(HybridTimestamp timestamp) throws StorageException {
        return delegate.scan(timestamp);
    }

    @Override
    public @Nullable RowId closestRowId(RowId lowerBound) throws StorageException {
        return delegate.closestRowId(lowerBound);
    }

    @SuppressWarnings("deprecation")
    @Override
    public long rowsCount() throws StorageException {
        return delegate.rowsCount();
    }

    @SuppressWarnings("deprecation")
    @Override
    public void forEach(BiConsumer<RowId, BinaryRow> consumer) {
        delegate.forEach(consumer);
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}
