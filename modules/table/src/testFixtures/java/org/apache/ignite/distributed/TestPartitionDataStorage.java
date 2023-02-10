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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.storage.RaftGroupConfiguration;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.jetbrains.annotations.Nullable;

/**
 * Test implementation of {@link PartitionDataStorage}.
 */
public class TestPartitionDataStorage implements PartitionDataStorage {
    private final MvPartitionStorage partitionStorage;

    private final Lock partitionSnapshotsLock = new ReentrantLock();

    public TestPartitionDataStorage(MvPartitionStorage partitionStorage) {
        this.partitionStorage = partitionStorage;
    }

    @Override
    public <V> V runConsistently(WriteClosure<V> closure) throws StorageException {
        return partitionStorage.runConsistently(closure);
    }

    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    @Override
    public void acquirePartitionSnapshotsReadLock() {
        partitionSnapshotsLock.lock();
    }

    @Override
    public void releasePartitionSnapshotsReadLock() {
        partitionSnapshotsLock.unlock();
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
    public @Nullable RaftGroupConfiguration committedGroupConfiguration() {
        return partitionStorage.committedGroupConfiguration();
    }

    @Override
    public void committedGroupConfiguration(RaftGroupConfiguration config) {
        partitionStorage.committedGroupConfiguration(config);
    }

    @Override
    public @Nullable BinaryRow addWrite(RowId rowId, @Nullable BinaryRow row, UUID txId, UUID commitTableId,
            int commitPartitionId) throws TxIdMismatchException, StorageException {
        return partitionStorage.addWrite(rowId, row, txId, commitTableId, commitPartitionId);
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
    public MvPartitionStorage getStorage() {
        return partitionStorage;
    }

    @Override
    public void close() {
        partitionStorage.close();
    }
}
