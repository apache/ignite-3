/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Interface providing methods to read, remove and update keys in storage. Any locking is unnecessary as this storage is used within RAFT
 * groups where all write operations are serialized.
 */
public interface PartitionStorage extends AutoCloseable {
    /**
     * Returns the partition id.
     *
     * @return Partition id.
     */
    int partitionId();

    /**
     * Reads a DataRow for a given key.
     *
     * @param key Search row.
     * @return Data row or {@code null} if no data has been found.
     * @throws StorageException If failed to read the data or the storage is already stopped.
     */
    @Nullable
    DataRow read(SearchRow key) throws StorageException;

    /**
     * Reads the value from the storage as it was at the given timestamp.
     *
     * @param key Key.
     * @param timestamp Timestamp.
     * @return Binary row that corresponds to the key or {@code null} if value is not found.
     */
    @Nullable
    default BinaryRow read(BinaryRow key, @Nullable Timestamp timestamp) {
        throw new UnsupportedOperationException("read");
    }

    /**
     * Reads {@link DataRow}s for a given collection of keys.
     *
     * @param keys Search rows.
     * @return Data rows.
     * @throws StorageException If failed to read the data or the storage is already stopped.
     */
    Collection<DataRow> readAll(List<? extends SearchRow> keys);

    /**
     * Writes a DataRow into the storage.
     *
     * @param row Data row.
     * @throws StorageException If failed to write the data or the storage is already stopped.
     */
    void write(DataRow row) throws StorageException;

    /**
     * Writes a collection of {@link DataRow}s into the storage.
     *
     * @param rows Data rows.
     * @throws StorageException If failed to write the data or the storage is already stopped.
     */
    void writeAll(List<? extends DataRow> rows) throws StorageException;

    /**
     * Inserts a collection of {@link DataRow}s into the storage and returns a collection of rows that can't be inserted due to their keys
     * being already present in the storage.
     *
     * @param rows Data rows.
     * @return Collection of rows that could not be inserted.
     * @throws StorageException If failed to write the data or the storage is already stopped.
     */
    Collection<DataRow> insertAll(List<? extends DataRow> rows) throws StorageException;

    /**
     * Removes a DataRow associated with a given Key.
     *
     * @param key Search row.
     * @throws StorageException If failed to remove the data or the storage is already stopped.
     */
    void remove(SearchRow key) throws StorageException;

    /**
     * Removes {@link DataRow}s mapped by given keys.
     *
     * @param keys Search rows.
     * @return List of skipped data rows.
     * @throws StorageException If failed to remove the data or the storage is already stopped.
     */
    Collection<SearchRow> removeAll(List<? extends SearchRow> keys);

    /**
     * Removes {@link DataRow}s mapped by given keys and containing given values.
     *
     * @param keyValues Data rows.
     * @return List of skipped data rows.
     * @throws StorageException If failed to remove the data or the storage is already stopped.
     */
    Collection<DataRow> removeAllExact(List<? extends DataRow> keyValues);

    /**
     * Executes an update with custom logic implemented by storage.UpdateClosure interface.
     *
     * @param key Search key.
     * @param clo Invoke closure.
     * @param <T> Closure invocation's result type.
     * @throws StorageException If failed to read data or storage is already stopped.
     */
    @Nullable
    <T> T invoke(SearchRow key, InvokeClosure<T> clo) throws StorageException;

    /**
     * Creates cursor over the storage data.
     *
     * @param filter Filter for the scan query.
     * @return Cursor with filtered data.
     * @throws StorageException If failed to read data or storage is already stopped.
     */
    Cursor<DataRow> scan(Predicate<SearchRow> filter) throws StorageException;

    /**
     * Scans the partition and returns a cursor of values in at the given timestamp.
     *
     * @param keyFilter Key filter. Binary rows passed to the filter may or may not have a value, filter should only check keys.
     * @param timestamp Timestamp.
     * @return Cursor.
     */
    default Cursor<BinaryRow> scan(Predicate<BinaryRow> keyFilter, @Nullable Timestamp timestamp) {
        throw new UnsupportedOperationException("scan");
    }

    /**
     * Exception class that describes the situation where two independant transactions attempting to write values for the same key.
     */
    class TxIdMismatchException extends RuntimeException {
    }

    /**
     * Creates uncommited version, assigned to the passed transaction id..
     *
     * @param row Binary row to update. Key only row means value removal.
     * @param txId Transaction id.
     * @throws TxIdMismatchException If there's another pending update associated with different transaction id.
     * @throws StorageException If failed to write data to the storage.
     */
    default void addWrite(BinaryRow row, UUID txId) throws TxIdMismatchException, StorageException {
        throw new UnsupportedOperationException("addWrite");
    }

    /**
     * Aborts a pending update of the ongoing uncommited transaction. Invoked during rollback.
     *
     * @param key Key.
     */
    default void abortWrite(BinaryRow key) {
        throw new UnsupportedOperationException("abortWrite");
    }

    /**
     * Commits a pending update of the ongoing transaction. Invoked during commit. Commited value will be versioned by the given timestamp.
     *
     * @param key Key.
     * @param timestamp Timestamp to associate with commited value.
     */
    default void commitWrite(BinaryRow key, Timestamp timestamp) {
        throw new UnsupportedOperationException("commitWrite");
    }

    /**
     * Removes data associated with old timestamps.
     *
     * @param from Start of hashes range to process. Inclusive.
     * @param to End of hashes range to process. Inclusive.
     * @param timestamp Timestamp to remove all the data with a lesser timestamp.
     * @return Future for the operation.
     */
    default CompletableFuture<?> cleanup(int from, int to, Timestamp timestamp) {
        throw new UnsupportedOperationException("cleanup");
    }

    /**
     * Creates a snapshot of the storage's current state in the specified directory.
     *
     * @param snapshotPath Directory to store a snapshot.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Void> snapshot(Path snapshotPath);

    /**
     * Restores a state of the storage which was previously captured with a {@link #snapshot(Path)}.
     *
     * @param snapshotPath Path to the snapshot's directory.
     */
    void restoreSnapshot(Path snapshotPath);

    /**
     * Removes all data from this storage and frees all associated resources.
     */
    void destroy();
}
