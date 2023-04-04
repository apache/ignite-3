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

package org.apache.ignite.internal.metastorage.server;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Defines key/value storage interface.
 */
public interface KeyValueStorage extends ManuallyCloseable {
    /**
     * Starts the given storage, allocating the necessary resources.
     */
    void start();

    /**
     * Returns storage revision.
     *
     * @return Storage revision.
     */
    long revision();

    /**
     * Returns update counter.
     *
     * @return Update counter.
     */
    long updateCounter();

    /**
     * Returns an entry by the given key.
     *
     * @param key The key.
     * @return Value corresponding to the given key.
     */
    Entry get(byte[] key);

    /**
     * Returns an entry by the given key and bounded by the given revision.
     *
     * @param key The key.
     * @param revUpperBound The upper bound of revision.
     * @return Value corresponding to the given key.
     */
    Entry get(byte[] key, long revUpperBound);

    /**
     * Returns all entries corresponding to given keys.
     *
     * @param keys Keys collection.
     * @return Entries corresponding to given keys.
     */
    Collection<Entry> getAll(List<byte[]> keys);

    /**
     * Returns all entries corresponding to given keys and bounded by the given revision.
     *
     * @param keys Keys collection.
     * @param revUpperBound Upper bound of revision.
     * @return Entries corresponding to given keys.
     */
    Collection<Entry> getAll(List<byte[]> keys, long revUpperBound);

    /**
     * Inserts an entry with the given key and given value.
     *
     * @param key The key.
     * @param value The value.
     */
    void put(byte[] key, byte[] value);

    /**
     * Inserts an entry with the given key and given value and returns previous entry.
     *
     * @param key The key.
     * @param value The value.
     * @return Previous entry corresponding to the given key.
     */
    Entry getAndPut(byte[] key, byte[] value);

    /**
     * Inserts entries with given keys and given values.
     *
     * @param keys The key list.
     * @param values The values list.
     */
    void putAll(List<byte[]> keys, List<byte[]> values);

    /**
     * Inserts entries with given keys and given values and returns previous entries.
     *
     * @param keys The key list.
     * @param values The values list.
     * @return Collection of previous entries corresponding to given keys.
     */
    Collection<Entry> getAndPutAll(List<byte[]> keys, List<byte[]> values);

    /**
     * Removes an entry with the given key.
     *
     * @param key The key.
     */
    void remove(byte[] key);

    /**
     * Removes an entry with the given key and returns previous entry.
     *
     * @param key The key.
     * @return Previous entry.
     */
    Entry getAndRemove(byte[] key);

    /**
     * Remove all entries corresponding to given keys.
     *
     * @param keys The keys list.
     */
    void removeAll(List<byte[]> keys);

    /**
     * Remove all entries corresponding to given keys and returns previous entries.
     *
     * @param keys The keys list.
     * @return Previous entries.
     */
    Collection<Entry> getAndRemoveAll(List<byte[]> keys);

    /**
     * Performs {@code success} operation if condition is {@code true}, otherwise performs {@code failure} operations.
     *
     * @param condition Condition.
     * @param success Success operations.
     * @param failure Failure operations.
     * @return Result of test condition.
     */
    boolean invoke(Condition condition, Collection<Operation> success, Collection<Operation> failure);

    /**
     * Invoke, which supports nested conditional statements with left and right branches of execution.
     *
     * @param iif {@link If} statement to invoke
     * @return execution result
     * @see If
     * @see StatementResult
     */
    StatementResult invoke(If iif);

    /**
     * Returns cursor by entries which correspond to the given keys range.
     *
     * @param keyFrom Start key of range (inclusive).
     * @param keyTo Last key of range (exclusive).
     * @return Cursor by entries which correspond to the given keys range.
     */
    Cursor<Entry> range(byte[] keyFrom, byte @Nullable [] keyTo);

    /**
     * Returns cursor by entries which correspond to the given keys range and bounded by revision number.
     *
     * @param keyFrom Start key of range (inclusive).
     * @param keyTo Last key of range (exclusive).
     * @param revUpperBound Upper bound of revision.
     * @return Cursor by entries which correspond to the given keys range.
     */
    Cursor<Entry> range(byte[] keyFrom, byte @Nullable [] keyTo, long revUpperBound);

    /**
     * Creates subscription on updates of entries corresponding to the given keys range and starting from the given revision number.
     *
     * @param keyFrom Start key of range (inclusive).
     * @param keyTo Last key of range (exclusive).
     * @param rev Start revision number.
     */
    void watchRange(byte[] keyFrom, byte @Nullable [] keyTo, long rev, WatchListener listener);

    /**
     * Registers a watch listener for the provided key.
     *
     * @param key Meta Storage key.
     * @param rev Starting Meta Storage revision.
     * @param listener Listener which will be notified for each update.
     */
    void watchExact(byte[] key, long rev, WatchListener listener);

    /**
     * Registers a watch listener for the provided keys.
     *
     * @param keys Meta Storage keys.
     * @param rev Starting Meta Storage revision.
     * @param listener Listener which will be notified for each update.
     */
    void watchExact(Collection<byte[]> keys, long rev, WatchListener listener);

    /**
     * Starts all registered watches.
     *
     * <p>Before calling this method, watches will not receive any updates.
     *
     * @param revisionCallback Callback that will be invoked after all watches of a particular revision are processed, with the
     *         revision and modified entries (processed by at least one watch) as its argument.
     */
    void startWatches(OnRevisionAppliedCallback revisionCallback);

    /**
     * Unregisters a watch listener.
     */
    void removeWatch(WatchListener listener);

    /**
     * Compacts storage (removes tombstones).
     * TODO: IGNITE-16444 Correct compaction for Meta storage.
     */
    void compact();

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
     * Returns the next possible key larger then the given one in lexicographic order. The returned key does not necessarily have to be
     * present in the storage.
     */
    byte @Nullable [] nextKey(byte[] key);
}
