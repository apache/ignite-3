/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.util.function.Predicate;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Multi-versioned partition storage. Maps RowId to a structures called "Version Chains". Each version chain is logically a stack of
 * elements with the following structure:
 * <pre><code>[timestamp | txId, row data]</code></pre>
 *
 * <p>Only the chain's head can contain a transaction id, every other element must have a timestamp. Presence of transaction id indicates
 * that the row is not yet committed.
 *
 * <p>All timestamps in the chain must go in decreasing order, giving us a N2O (newest to oldest) order of search.
 *
 * <p>Each MvPartitionStorage instance represents exactly one partition.
 */
public interface MvPartitionStorage extends AutoCloseable {
    /**
     * Closure for executing write operations on the storage.
     *
     * @param <V> Type of the result returned from the closure.
     */
    @SuppressWarnings("PublicInnerClass")
    @FunctionalInterface
    interface WriteClosure<V> {
        V execute() throws StorageException;
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
     * Flushes current state of the data or <i>the state from the nearest future</i> to the storage. It means that the future can be
     * completed when {@link #persistedIndex()} is higher than {@link #lastAppliedIndex()} at the moment of the method's call. This feature
     * allows implementing a batch flush for several partitions at once.
     *
     * @return Future that's completed when flushing of the data is completed.
     */
    CompletableFuture<Void> flush();

    /**
     * Index of the highest write command applied to the storage. {@code 0} if index is unknown.
     */
    long lastAppliedIndex();

    /**
     * Sets the last applied index value.
     */
    void lastAppliedIndex(long lastAppliedIndex) throws StorageException;

    /**
     * {@link #lastAppliedIndex()} value consistent with the data, already persisted on the storage.
     */
    long persistedIndex();

    /**
     * Reads either the committed value from the storage or the uncommitted value belonging to given transaction.
     *
     * @param rowId Row id.
     * @param txId Transaction id.
     * @return Binary row that corresponds to the key or {@code null} if value is not found.
     * @throws TxIdMismatchException If there's another pending update associated with different transaction id.
     * @throws StorageException If failed to read data from the storage.
     */
    @Nullable
    BinaryRow read(RowId rowId, UUID txId) throws TxIdMismatchException, StorageException;

    /**
     * Reads the value from the storage as it was at the given timestamp.
     *
     * @param rowId Row id.
     * @param timestamp Timestamp.
     * @return Binary row that corresponds to the key or {@code null} if value is not found.
     */
    @Nullable
    BinaryRow read(RowId rowId, Timestamp timestamp) throws StorageException;

    /**
     * Creates an uncommitted version, assigning a new row id to it.
     *
     * @param binaryRow Binary row to insert. Not null.
     * @param txId Transaction id.
     * @return Row id.
     * @throws StorageException If failed to write data into the storage.
     *
     * @deprecated Generates different ids for each replica. {@link #addWrite(RowId, BinaryRow, UUID)} with explicit replicated id must be
     *      used instead.
     */
    @Deprecated
    RowId insert(BinaryRow binaryRow, UUID txId) throws StorageException;

    /**
     * Creates (or replaces) an uncommitted (aka pending) version, assigned to the given transaction id.
     * In details:
     * - if there is no uncommitted version, a new uncommitted version is added
     * - if there is an uncommitted version belonging to the same transaction, it gets replaced by the given version
     * - if there is an uncommitted version belonging to a different transaction, {@link TxIdMismatchException} is thrown
     *
     * @param rowId Row id.
     * @param row Binary row to update. Key only row means value removal.
     * @param txId Transaction id.
     * @return Previous uncommitted row version associated with the row id, or {@code null} if no uncommitted version
     *     exists before this call
     * @throws TxIdMismatchException If there's another pending update associated with different transaction id.
     * @throws StorageException If failed to write data to the storage.
     */
    @Nullable BinaryRow addWrite(RowId rowId, @Nullable BinaryRow row, UUID txId) throws TxIdMismatchException, StorageException;

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
    void commitWrite(RowId rowId, Timestamp timestamp) throws StorageException;

    /**
     * Scans the partition and returns a cursor of values. All filtered values must either be uncommitted in current transaction
     * or already committed in different transaction.
     *
     * @param keyFilter Key filter. Binary rows passed to the filter may or may not have a value, filter should only check keys.
     * @param txId Transaction id.
     * @return Cursor.
     * @throws StorageException If failed to read data from the storage.
     */
    Cursor<BinaryRow> scan(Predicate<BinaryRow> keyFilter, UUID txId) throws TxIdMismatchException, StorageException;

    /**
     * Scans the partition and returns a cursor of values at the given timestamp.
     *
     * @param keyFilter Key filter. Binary rows passed to the filter may or may not have a value, filter should only check keys.
     * @param timestamp Timestamp. Can't be {@code null}.
     * @return Cursor.
     * @throws TxIdMismatchException If there's another pending update associated with different transaction id.
     * @throws StorageException If failed to read data from the storage.
     */
    Cursor<BinaryRow> scan(Predicate<BinaryRow> keyFilter, Timestamp timestamp) throws StorageException;

    /**
     * Returns rows count belongs to current storage.
     *
     * @return Rows count.
     * @throws StorageException If failed to obtain size.
     * @deprecated It's not yet defined what a "count" is. This value is not easily defined for multiversioned storages.
     *      TODO IGNITE-16769 Implement correct PartitionStorage rows count calculation.
     */
    @Deprecated
    long rowsCount() throws StorageException;

    /**
     * Iterates over all versions of all entries, except for tombstones.
     *
     * @param consumer Closure to process entries.
     * @deprecated This method was bord out of desperation and isn't well-designed. Implementation is not polished either. Currently, it's
     *      only usage is to work-around in-memory PK index rebuild on node restart, which shouldn't even exist in the first place.
     */
    @Deprecated
    void forEach(BiConsumer<RowId, BinaryRow> consumer);
}
