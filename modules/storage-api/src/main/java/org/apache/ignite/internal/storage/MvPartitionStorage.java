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
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.gc.GcEntry;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Multi-versioned partition storage. Maps RowId to a structures called "Version Chains". Each version chain is logically a stack of
 * elements with the following structure:
 * <pre>{@code [timestamp | transaction state (txId + commitTableId + commitPartitionId), row data]}</pre>
 *
 * <p>Only the chain's head can contain a transaction state, every other element must have a timestamp. Presence of transaction state
 * indicates that the row is not yet committed.
 *
 * <p>All timestamps in the chain must go in decreasing order, giving us a N2O (newest to oldest) order of search.
 *
 * <p>Each MvPartitionStorage instance represents exactly one partition. All RowIds within a partition are sorted consistently with the
 * {@link RowId#compareTo} comparison order.
 */
public interface MvPartitionStorage extends ManuallyCloseable {
    /**
     * Value of the {@link #lastAppliedIndex()} and {@link #lastAppliedTerm()} during rebalance of transaction state storage.
     *
     * <p>Allows to determine on a node restart that rebalance has not been completed and storage should be cleared before using it.
     */
    long REBALANCE_IN_PROGRESS = -1;

    /**
     * Closure for executing write operations on the storage. All write operations, such as
     * {@link #addWrite(RowId, BinaryRow, UUID, int, int)} or {@link #commitWrite(RowId, HybridTimestamp)},
     * as well as {@link #scanVersions(RowId)}, and operations like {@link #committedGroupConfiguration(byte[])}, must be executed inside
     * of the write closure. Also, each operation that involves modifying rows (and {@link #scanVersions(RowId)}) must hold lock on
     * the corresponding row ID, by either calling {@link Locker#lock(RowId)} or calling {@link Locker#tryLock(RowId)} and checking the
     * result.
     *
     * @param <V> Type of the result returned from the closure.
     */
    @SuppressWarnings("PublicInnerClass")
    @FunctionalInterface
    interface WriteClosure<V> {
        V execute(Locker locker) throws StorageException;
    }

    /**
     * Parameter type for {@link WriteClosure#execute(Locker)}. Used to lock row IDs before updating the data. All acquired locks are
     * released automatically after {@code execute} call is completed.
     */
    @SuppressWarnings("PublicInnerClass")
    interface Locker {
        /**
         * Locks passed row ID until the {@link WriteClosure#execute(Locker)} is completed.
         * If the lock is already held by another thread, current thread will wait until the lock is released to acquire it.
         *
         * @param rowId Row ID to lock.
         */
        void lock(RowId rowId);

        /**
         * Tries to lock passed row ID. If successful, lock will be released when the {@link WriteClosure#execute(Locker)} is completed.
         *
         * @param rowId Row ID to lock.
         * @return {@code true} if row ID has been locked successfully, or the lock has already been held by current thread.
         *      {@code false} if lock is not held by the current thread and the attempt to acquire it has failed.
         */
        boolean tryLock(RowId rowId);
    }

    /**
     * Executes {@link WriteClosure} atomically, meaning that partial result of an incomplete closure will never be written to the
     * physical device, thus guaranteeing data consistency after restart. Simply runs the closure in case of a volatile storage.
     *
     * @param closure Data access closure to be executed.
     * @param <V> Type of the result returned from the closure.
     * @return Closure result.
     * @throws StorageException If failed to write data to the storage.
     */
    <V> V runConsistently(WriteClosure<V> closure) throws StorageException;

    /**
     * Flushes current state of the data or <i>the state from the nearest future</i> to the storage.
     * This feature allows implementing a batch flush for several partitions at once.
     *
     * @return Future that's completed when flushing of the data is completed.
     */
    CompletableFuture<Void> flush();

    /**
     * Index of the write command with the highest index applied to the storage. {@code 0} if the index is unknown.
     */
    long lastAppliedIndex();

    /**
     * Term of the write command with the highest index applied to the storage. {@code 0} if the term is unknown.
     */
    long lastAppliedTerm();

    /**
     * Sets the last applied index and term.
     *
     * @param lastAppliedIndex Last applied index value.
     * @param lastAppliedTerm Last applied term value.
     */
    void lastApplied(long lastAppliedIndex, long lastAppliedTerm) throws StorageException;

    /**
     * Byte representation of the committed replication protocol group configuration corresponding to the write command with the highest
     * index applied to the storage.
     * {@code null} if it was never saved.
     */
    byte @Nullable [] committedGroupConfiguration();

    /**
     * Updates RAFT group configuration.
     *
     * @param config Byte representation of the configuration to save.
     */
    void committedGroupConfiguration(byte[] config);

    /**
     * Reads the value from the storage as it was at the given timestamp.
     * If there is a row with specified row id and timestamp - return it.
     * If there are multiple versions of row with specified row id:
     * <ol>
     *     <li>If there is only write-intent - return write-intent.</li>
     *     <li>If there is write-intent and previous commit is older than timestamp - return write-intent.</li>
     *     <li>If there is a commit older than timestamp, but no write-intent - return said commit.</li>
     *     <li>If there are two commits one older and one newer than timestamp - return older commit.</li>
     *     <li>There are commits but they're all newer than timestamp - return nothing.</li>
     * </ol>
     *
     * <p>{@link ReadResult#newestCommitTimestamp()} is filled by this method for intents having preceding committed
     * versions.
     *
     * @param rowId Row id.
     * @param timestamp Timestamp.
     * @return Read result that corresponds to the key.
     */
    ReadResult read(RowId rowId, HybridTimestamp timestamp) throws StorageException;

    /**
     * Creates (or replaces) an uncommitted (aka pending) version, assigned to the given transaction id.
     * In details:
     * - if there is no uncommitted version, a new uncommitted version is added
     * - if there is an uncommitted version belonging to the same transaction, it gets replaced by the given version
     * - if there is an uncommitted version belonging to a different transaction, {@link TxIdMismatchException} is thrown
     *
     * @param rowId Row id.
     * @param row Table row to update. {@code null} means value removal.
     * @param txId Transaction id.
     * @param commitTableId Commit table id.
     * @param commitPartitionId Commit partitionId.
     * @return Previous uncommitted row version associated with the row id, or {@code null} if no uncommitted version
     *     exists before this call
     * @throws TxIdMismatchException If there's another pending update associated with different transaction id.
     * @throws StorageException If failed to write data to the storage.
     */
    @Nullable BinaryRow addWrite(
            RowId rowId,
            @Nullable BinaryRow row,
            UUID txId,
            int commitZoneId,
            int commitTableId,
            int commitPartitionId
    ) throws TxIdMismatchException, StorageException;

    /**
     * Aborts a pending update of the ongoing uncommitted transaction. Invoked during rollback.
     *
     * @param rowId Row id.
     * @return Previous uncommitted row version associated with the row id.
     * @throws StorageException If failed to write data to the storage.
     */
    @Nullable BinaryRow abortWrite(RowId rowId) throws StorageException;

    /**
     * Commits a pending update of the ongoing transaction. Invoked during commit. Committed value will be versioned by the given timestamp.
     *
     * @param rowId Row id.
     * @param timestamp Timestamp to associate with committed value.
     * @throws StorageException If failed to write data to the storage.
     */
    void commitWrite(RowId rowId, HybridTimestamp timestamp) throws StorageException;

    /**
     * Creates a committed version.
     * In details:
     * - if there is no uncommitted version, a new committed version is added
     * - if there is an uncommitted version, this method may fail with a system exception (this method should not be called if there
     *   is already something uncommitted for the given row).
     *
     * @param rowId Row id.
     * @param row Table row to update. Key only row means value removal.
     * @param commitTimestamp Timestamp to associate with committed value.
     * @throws StorageException If failed to write data to the storage.
     */
    void addWriteCommitted(RowId rowId, @Nullable BinaryRow row, HybridTimestamp commitTimestamp) throws StorageException;

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
     * Scans the partition and returns a cursor of values at the given timestamp. This cursor filters out committed tombstones, but not
     * tombstones in the write-intent state.
     *
     * @param timestamp Timestamp. Can't be {@code null}.
     * @return Cursor.
     * @throws TxIdMismatchException If there's another pending update associated with different transaction id.
     * @throws StorageException If failed to read data from the storage.
     */
    PartitionTimestampCursor scan(HybridTimestamp timestamp) throws StorageException;

    /**
     * Returns a row id, existing in the storage, that's greater or equal than the lower bound. {@code null} if not found.
     *
     * @param lowerBound Lower bound.
     * @throws StorageException If failed to read data from the storage.
     */
    @Nullable RowId closestRowId(RowId lowerBound) throws StorageException;

    /**
     * Returns the head of GC queue.
     *
     * @param lowWatermark Upper bound for commit timestamp of GC entry, inclusive.
     * @return Queue head or {@code null} if there are no entries below passed low watermark.
     */
    @Nullable GcEntry peek(HybridTimestamp lowWatermark);

    /**
     * Delete GC entry from the GC queue and corresponding version chain. Row ID of the entry must be locked to call this method.
     *
     * @param entry Entry, previously returned by {@link #peek(HybridTimestamp)}.
     * @return Polled binary row, or {@code null} if the entry has already been deleted by another thread.
     *
     * @see Locker#lock(RowId)
     * @see Locker#tryLock(RowId)
     */
    @Nullable BinaryRow vacuum(GcEntry entry);

    /**
     * Returns rows count belongs to current storage.
     *
     * @return Rows count.
     * @throws StorageException If failed to obtain size.
     * @deprecated It's not yet defined what a "count" is. This value is not easily defined for multi-versioned storages.
     *      TODO IGNITE-16769 Implement correct PartitionStorage rows count calculation.
     */
    @Deprecated
    long rowsCount() throws StorageException;

    /**
     * Updates the current lease start time in the storage.
     *
     * @param leaseStartTime Lease start time.
     */
    void updateLease(long leaseStartTime);

    /**
     * Return the start time of the known lease for this replication group.
     *
     * @return Lease start time.
     */
    long leaseStartTime();

    /**
     * Closes the storage.
     *
     * <p>REQUIRED: For background tasks for partition, such as rebalancing, to be completed by the time the method is called.
     */
    @Override
    void close();
}
