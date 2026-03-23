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

package org.apache.ignite.internal.partition.replicator.raft.snapshot;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.PartitionSnapshots;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
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
 * Provides access to MV (multi-version) data of a table partition.
 *
 * <p>Methods writing to MV storage ({@link #addWrite(RowId, BinaryRow, UUID, int, int)}, {@link #abortWrite}
 * and {@link #commitWrite}) and TX data storage MUST be invoked under a lock acquired using
 * {@link PartitionSnapshots#acquireReadLock()}.
 *
 * <p>Each MvPartitionStorage instance represents exactly one partition. All RowIds within a partition are sorted consistently with the
 * {@link RowId#compareTo} comparison order.
 *
 * @see MvPartitionStorage
 */
public interface PartitionDataStorage extends ManuallyCloseable {
    /** Returns table ID. */
    int tableId();

    /** Returns partition ID. */
    int partitionId();

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
     * Flushes current state of the data or <i>the state from the nearest future</i> to the storage.
     * This feature allows implementing a batch flush for several partitions at once.
     *
     * @return Future that's completed when flushing of the data is completed.
     * @see MvPartitionStorage#flush()
     */
    default CompletableFuture<Void> flush() {
        return flush(true);
    }

    /**
     * Flushes current state of the data or <i>the state from the nearest future</i> to the storage.
     * This feature allows implementing a batch flush for several partitions at once.
     *
     * @param trigger {@code true} if the flush should be explicitly triggered, otherwise
     *         the future for the next scheduled flush will be returned.
     * @see MvPartitionStorage#flush(boolean)
     */
    CompletableFuture<Void> flush(boolean trigger);

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
     * Updates RAFT group configuration.
     *
     * @param config Configuration to save.
     * @see MvPartitionStorage#committedGroupConfiguration(byte[])
     */
    void committedGroupConfiguration(RaftGroupConfiguration config);

    /**
     * Returns the last saved RAFT group configuration or {@code null} if it has never been saved.
     */
    @Nullable RaftGroupConfiguration committedGroupConfiguration();

    /**
     * Creates (or replaces) an uncommitted (aka pending) version, assigned to the given transaction ID.
     *
     * <p>In details:</p>
     * <ul>
     * <li>If there is no uncommitted version, a new uncommitted version is added.</li>
     * <li>If there is an uncommitted version belonging to the same transaction, it gets replaced by the given version.</li>
     * <li>If there is an uncommitted version belonging to a different transaction, nothing will happen.</li>
     * </ul>
     *
     * @param rowId Row ID.
     * @param row Table row to update. {@code null} means value removal.
     * @param txId Transaction ID.
     * @param commitZoneId Commit zone ID.
     * @param commitPartitionId Commit partition ID.
     * @return Result of add write intent.
     * @throws StorageException If failed to write data to the storage.
     * @see MvPartitionStorage#addWrite
     */
    AddWriteResult addWrite(
            RowId rowId,
            @Nullable BinaryRow row,
            UUID txId,
            int commitZoneId,
            int commitPartitionId
    ) throws StorageException;

    /**
     * Creates a committed version.
     *
     * <p>In details:</p>
     * <ul>
     * <li>If there is no uncommitted version, a new committed version is added.</li>
     * <li>If there is an uncommitted version, nothing will happen.</li>
     * </ul>
     *
     * @param rowId Row ID.
     * @param row Table row to update. Key only row means value removal.
     * @param commitTimestamp Timestamp to associate with committed value.
     * @return Result of add write intent committed.
     * @throws StorageException If failed to write data to the storage.
     * @see MvPartitionStorage#addWriteCommitted
     */
    AddWriteCommittedResult addWriteCommitted(
            RowId rowId,
            @Nullable BinaryRow row,
            HybridTimestamp commitTimestamp
    ) throws StorageException;

    /**
     * Aborts a pending update of the ongoing uncommitted transaction. Invoked during rollback.
     *
     * <p>This must be called under a lock acquired using {@link PartitionSnapshots#acquireReadLock()}.
     *
     * @param rowId Row ID.
     * @param txId Transaction ID that abort write intent.
     * @return Result of abort write intent.
     * @throws StorageException If failed to write data to the storage.
     * @see MvPartitionStorage#abortWrite
     */
    AbortResult abortWrite(RowId rowId, UUID txId) throws StorageException;

    /**
     * Commits a pending update of the ongoing transaction. Invoked during commit. Committed value will be versioned by the given timestamp.
     *
     * <p>This must be called under a lock acquired using {@link PartitionSnapshots#acquireReadLock()}.
     *
     * @param rowId Row ID.
     * @param timestamp Timestamp to associate with committed value.
     * @param txId Transaction ID that commit write intent.
     * @return Result of commit write intent.
     * @throws StorageException If failed to write data to the storage.
     * @see MvPartitionStorage#commitWrite
     */
    CommitResult commitWrite(RowId rowId, HybridTimestamp timestamp, UUID txId) throws StorageException;

    /**
     * Scans all versions of a single row.
     *
     * <p>{@link ReadResult#newestCommitTimestamp()} is NOT filled by this method for intents having preceding committed
     * versions.
     *
     * @param rowId Row id.
     * @return Cursor of results including both rows data and transaction-related context. The versions are ordered from newest to oldest.
     */
    Cursor<ReadResult> scanVersions(RowId rowId) throws StorageException;

    /**
     * Returns the underlying {@link MvPartitionStorage}. Only for tests!
     *
     * @return Underlying {@link MvPartitionStorage}.
     */
    MvPartitionStorage getStorage();

    /**
     * Closes the storage.
     */
    @Override
    void close();

    /**
     * Scans the partition and returns a cursor of values at the given timestamp. This cursor filters out committed tombstones, but not
     * tombstones in the write-intent state.
     *
     * @param timestamp Timestamp. Can't be {@code null}.
     * @return Cursor.
     * @throws StorageException If failed to read data from the storage.
     */
    PartitionTimestampCursor scan(HybridTimestamp timestamp) throws StorageException;

    /**
     * Returns entries from the queue starting from the head.
     *
     * @see MvPartitionStorage#peek
     */
    List<GcEntry> peek(HybridTimestamp lowWatermark, int count);

    /**
     * Delete GC entry from the GC queue and corresponding version chain.
     *
     * @see MvPartitionStorage#vacuum(GcEntry)
     */
    @Nullable BinaryRow vacuum(GcEntry entry);

    /**
     * Updates the current lease information in the storage.
     */
    void updateLease(LeaseInfo leaseInfo);

    /**
     * Return the information about the known lease for this replication group.
     */
    @Nullable LeaseInfo leaseInfo();
}
