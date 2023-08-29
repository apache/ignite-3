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
import java.util.function.LongConsumer;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.RevisionUpdateListener;
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
     * Returns all entries corresponding to the given key and bounded by given revisions.
     * All these entries are ordered by revisions and have the same key.
     * The lower bound and the upper bound are inclusive.
     *
     * @param key The key.
     * @param revLowerBound The lower bound of revision.
     * @param revUpperBound The upper bound of revision.
     * @return Entries corresponding to the given key.
     */
    List<Entry> get(byte[] key, long revLowerBound, long revUpperBound);

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
     * @param opTs Operation's timestamp.
     */
    void put(byte[] key, byte[] value, HybridTimestamp opTs);

    /**
     * Inserts an entry with the given key and given value and returns previous entry.
     *
     * @param key The key.
     * @param value The value.
     * @param opTs Operation's timestamp.
     * @return Previous entry corresponding to the given key.
     */
    Entry getAndPut(byte[] key, byte[] value, HybridTimestamp opTs);

    /**
     * Inserts entries with given keys and given values.
     *
     * @param keys The key list.
     * @param values The values list.
     * @param opTs Operation's timestamp.
     */
    void putAll(List<byte[]> keys, List<byte[]> values, HybridTimestamp opTs);

    /**
     * Inserts entries with given keys and given values and returns previous entries.
     *
     * @param keys The key list.
     * @param values The values list.
     * @param opTs Operation's timestamp.
     * @return Collection of previous entries corresponding to given keys.
     */
    Collection<Entry> getAndPutAll(List<byte[]> keys, List<byte[]> values, HybridTimestamp opTs);

    /**
     * Removes an entry with the given key.
     *
     * @param key The key.
     * @param opTs Operation's timestamp.
     */
    void remove(byte[] key, HybridTimestamp opTs);

    /**
     * Removes an entry with the given key and returns previous entry.
     *
     * @param key The key.
     * @param opTs Operation's timestamp.
     * @return Previous entry.
     */
    Entry getAndRemove(byte[] key, HybridTimestamp opTs);

    /**
     * Remove all entries corresponding to given keys.
     *
     * @param keys The keys list.
     * @param opTs Operation's timestamp.
     */
    void removeAll(List<byte[]> keys, HybridTimestamp opTs);

    /**
     * Remove all entries corresponding to given keys and returns previous entries.
     *
     * @param keys The keys list.
     * @param opTs Operation's timestamp.
     * @return Previous entries.
     */
    Collection<Entry> getAndRemoveAll(List<byte[]> keys, HybridTimestamp opTs);

    /**
     * Performs {@code success} operation if condition is {@code true}, otherwise performs {@code failure} operations.
     *
     * @param condition Condition.
     * @param success Success operations.
     * @param failure Failure operations.
     * @param opTs Operation's timestamp.
     * @return Result of test condition.
     */
    boolean invoke(Condition condition, Collection<Operation> success, Collection<Operation> failure, HybridTimestamp opTs);

    /**
     * Invoke, which supports nested conditional statements with left and right branches of execution.
     *
     * @param iif {@link If} statement to invoke
     * @param opTs Operation's timestamp.
     * @return execution result
     * @see If
     * @see StatementResult
     */
    StatementResult invoke(If iif, HybridTimestamp opTs);

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
     * @param startRevision Revision to start processing updates from.
     * @param revisionCallback Callback that will be invoked after all watches of a particular revision are processed, with the
     *         revision and modified entries (processed by at least one watch) as its argument.
     */
    void startWatches(long startRevision, OnRevisionAppliedCallback revisionCallback);

    /**
     * Unregisters a watch listener.
     */
    void removeWatch(WatchListener listener);

    /**
     * Compacts storage (removes tombstones).
     *
     * @param lowWatermark A time threshold for the entry. Only entries that have revisions with timestamp higher or equal to the
     *     watermark can be removed.
     */
    // TODO: IGNITE-16444 Correct compaction for Meta storage.
    // TODO: IGNITE-19417 Provide low-watermark for compaction.
    void compact(HybridTimestamp lowWatermark);

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

    /**
     * Looks up a timestamp by a revision.
     *
     * @param revision Revision by which to do a lookup.
     * @return Timestamp corresponding to the revision.
     */
    HybridTimestamp timestampByRevision(long revision);

    /**
     * Sets the revision listener. This is needed only for the recovery, after that listener must be set to {@code null}.
     * {@code null} means that we no longer must be notified of revision updates for recovery, because recovery is finished.
     *
     * @param listener Revision listener.
     */
    void setRecoveryRevisionListener(@Nullable LongConsumer listener);

    /** Registers a Meta Storage revision update listener. */
    void registerRevisionUpdateListener(RevisionUpdateListener listener);

    /** Unregisters a Meta Storage revision update listener. */
    void unregisterRevisionUpdateListener(RevisionUpdateListener listener);

    /** Explicitly notifies revision update listeners. */
    CompletableFuture<Void> notifyRevisionUpdateListenerOnStart(long newRevision);

    /**
     * Advances MetaStorage Safe Time to a new value without creating a new revision.
     *
     * @param newSafeTime New Safe Time value.
     */
    void advanceSafeTime(HybridTimestamp newSafeTime);
}
