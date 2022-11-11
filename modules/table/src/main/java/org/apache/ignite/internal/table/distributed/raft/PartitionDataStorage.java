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

package org.apache.ignite.internal.table.distributed.raft;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.storage.RaftGroupConfiguration;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Provides access to MV (multi-version) data of a partition.
 *
 * <p>Methods writing to MV storage ({@link #addWrite(RowId, BinaryRow, UUID, UUID, int)}, {@link #abortWrite(RowId)}
 * and {@link #commitWrite(RowId, HybridTimestamp)}) and TX data storage MUST be invoked under a lock acquired using
 * {@link #acquirePartitionSnapshotsReadLock()}.
 *
 * <p>Each MvPartitionStorage instance represents exactly one partition. All RowIds within a partition are sorted consistently with the
 * {@link RowId#compareTo} comparison order.
 *
 * @see MvPartitionStorage
 */
public interface PartitionDataStorage extends ManuallyCloseable {
    /**
     * Executes {@link WriteClosure} atomically, meaning that partial result of an incomplete closure will never be written to the
     * physical device, thus guaranteeing data consistency after restart. Simply runs the closure in case of a volatile storage.
     *
     * @param closure Data access closure to be executed.
     * @param <V> Type of the result returned from the closure.
     * @return Closure result.
     * @throws StorageException If failed to write data to the storage.
     * @see MvPartitionStorage#runConsistently(WriteClosure)
     */
    <V> V runConsistently(WriteClosure<V> closure) throws StorageException;

    /**
     * Acquires the read lock on partition snapshots.
     */
    void acquirePartitionSnapshotsReadLock();

    /**
     * Releases the read lock on partition snapshots.
     */
    void releasePartitionSnapshotsReadLock();

    /**
     * Flushes current state of the data or <i>the state from the nearest future</i> to the storage. It means that the future can be
     * completed when the underlying storage {@link MvPartitionStorage#persistedIndex()} is higher
     * than {@link #lastAppliedIndex()} at the moment of the method's call. This feature
     * allows implementing a batch flush for several partitions at once.
     *
     * @return Future that's completed when flushing of the data is completed.
     * @see MvPartitionStorage#flush()
     */
    CompletableFuture<Void> flush();

    /**
     * Index of the write command with the highest index applied to the storage. {@code 0} if index is unknown.
     *
     * @see MvPartitionStorage#lastAppliedIndex()
     */
    long lastAppliedIndex();

    /**
     * Term of the write command with the highest index applied to the storage. {@code 0} if index is unknown.
     *
     * @see MvPartitionStorage#lastAppliedTerm()
     */
    long lastAppliedTerm();

    /**
     * Sets the last applied index and term.
     *
     * @see MvPartitionStorage#lastApplied(long, long)
     */
    void lastApplied(long lastAppliedIndex, long lastAppliedTerm) throws StorageException;

    /**
     * Committed RAFT group configuration corresponding to the write command with the highest index applied to the storage.
     * {@code null} if it was never saved.
     *
     * @see MvPartitionStorage#committedGroupConfiguration()
     */
    @Nullable
    RaftGroupConfiguration committedGroupConfiguration();

    /**
     * Updates RAFT group configuration.
     *
     * @param config Configuration to save.
     * @see MvPartitionStorage#committedGroupConfiguration(RaftGroupConfiguration)
     */
    void committedGroupConfiguration(RaftGroupConfiguration config);

    /**
     * Creates (or replaces) an uncommitted (aka pending) version, assigned to the given transaction id.
     * In details:
     * - if there is no uncommitted version, a new uncommitted version is added
     * - if there is an uncommitted version belonging to the same transaction, it gets replaced by the given version
     * - if there is an uncommitted version belonging to a different transaction, {@link TxIdMismatchException} is thrown
     *
     * <p>This must be called under a lock acquired using {@link #acquirePartitionSnapshotsReadLock()}.
     *
     * @param rowId Row id.
     * @param row Binary row to update. Key only row means value removal.
     * @param txId Transaction id.
     * @param commitTableId Commit table id.
     * @param commitPartitionId Commit partitionId.
     * @return Previous uncommitted row version associated with the row id, or {@code null} if no uncommitted version
     *     exists before this call
     * @throws TxIdMismatchException If there's another pending update associated with different transaction id.
     * @throws StorageException If failed to write data to the storage.
     * @see MvPartitionStorage#addWrite(RowId, BinaryRow, UUID, UUID, int)
     */
    @Nullable BinaryRow addWrite(RowId rowId, @Nullable BinaryRow row, UUID txId, UUID commitTableId, int commitPartitionId)
            throws TxIdMismatchException, StorageException;

    /**
     * Aborts a pending update of the ongoing uncommitted transaction. Invoked during rollback.
     *
     * <p>This must be called under a lock acquired using {@link #acquirePartitionSnapshotsReadLock()}.
     *
     * @param rowId Row id.
     * @return Previous uncommitted row version associated with the row id.
     * @throws StorageException If failed to write data to the storage.
     * @see MvPartitionStorage#abortWrite(RowId)
     */
    @Nullable BinaryRow abortWrite(RowId rowId) throws StorageException;

    /**
     * Commits a pending update of the ongoing transaction. Invoked during commit. Committed value will be versioned by the given timestamp.
     *
     * <p>This must be called under a lock acquired using {@link #acquirePartitionSnapshotsReadLock()}.
     *
     * @param rowId Row id.
     * @param timestamp Timestamp to associate with committed value.
     * @throws StorageException If failed to write data to the storage.
     * @see MvPartitionStorage#commitWrite(RowId, HybridTimestamp)
     */
    void commitWrite(RowId rowId, HybridTimestamp timestamp) throws StorageException;

    /**
     * Returns the underlying {@link MvPartitionStorage}. Only for tests!
     *
     * @return Underlying {@link MvPartitionStorage}.
     */
    @TestOnly
    MvPartitionStorage getStorage();

    /**
     * Closes the storage.
     */
    @Override
    void close();
}
