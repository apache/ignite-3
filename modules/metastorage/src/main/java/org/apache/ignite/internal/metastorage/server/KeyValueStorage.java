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
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.CommandId;
import org.apache.ignite.internal.metastorage.CompactionRevisionUpdateListener;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.RevisionUpdateListener;
import org.apache.ignite.internal.metastorage.Revisions;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.exceptions.MetaStorageException;
import org.apache.ignite.internal.raft.IndexWithTerm;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Defines key/value storage interface.
 */
public interface KeyValueStorage extends ManuallyCloseable {
    byte[] INVOKE_RESULT_TRUE_BYTES = {ByteUtils.booleanToByte(true)};
    byte[] INVOKE_RESULT_FALSE_BYTES = {ByteUtils.booleanToByte(false)};

    /**
     * Starts the given storage, allocating the necessary resources.
     */
    void start();

    /** Returns storage revision, {@code 0} if there have been no storage update operations yet. */
    long revision();

    /**
     * Returns the latest version of an entry by key.
     *
     * <p>Never throws {@link CompactedException}.</p>
     *
     * @param key Key.
     */
    Entry get(byte[] key);

    /**
     * Returns an entry by the given key and bounded by the given revision.
     *
     * <p>Let's consider examples of the work of the method and compaction of the metastorage. Let's assume that we have keys with
     * revisions "foo" [1, 2] and "bar" [1, 2 (tombstone)], and the key "some" has never been in the metastorage.</p>
     * <ul>
     *     <li>Compaction revision is {@code 1}.
     *     <ul>
     *         <li>get("foo", 1) - a {@link CompactedException} will be thrown.</li>
     *         <li>get("foo", 2) - will return a single value with revision 2.</li>
     *         <li>get("foo", 3) - will return a single value with revision 2.</li>
     *         <li>get("bar", 1) - a {@link CompactedException} will be thrown.</li>
     *         <li>get("bar", 2) - will return a single value with revision 2.</li>
     *         <li>get("bar", 3) - will return a single value with revision 2.</li>
     *         <li>get("some", 1) - a {@link CompactedException} will be thrown.</li>
     *         <li>get("some", 2) - will return an empty value.</li>
     *         <li>get("some", 3) - will return an empty value.</li>
     *     </ul>
     *     </li>
     *     <li>Compaction revision is {@code 2}.
     *     <ul>
     *         <li>get("foo", 1) - a {@link CompactedException} will be thrown.</li>
     *         <li>get("foo", 2) - will return a single value with revision 2.</li>
     *         <li>get("foo", 3) - will return a single value with revision 2.</li>
     *         <li>get("bar", 1) - a {@link CompactedException} will be thrown.</li>
     *         <li>get("bar", 2) - a {@link CompactedException} will be thrown.</li>
     *         <li>get("bar", 3) - will return a single value with revision 2.</li>
     *         <li>get("some", 1) - a {@link CompactedException} will be thrown.</li>
     *         <li>get("some", 2) - a {@link CompactedException} will be thrown.</li>
     *         <li>get("some", 3) - will return an empty value.</li>
     *     </ul>
     *     </li>
     *     <li>Compaction revision is {@code 3}.
     *     <ul>
     *         <li>get("foo", 1) - a {@link CompactedException} will be thrown.</li>
     *         <li>get("foo", 2) - will return a single value with revision 2.</li>
     *         <li>get("foo", 3) - will return a single value with revision 2.</li>
     *         <li>get("bar", 1) - a {@link CompactedException} will be thrown.</li>
     *         <li>get("bar", 2) - a {@link CompactedException} will be thrown.</li>
     *         <li>get("bar", 3) - a {@link CompactedException} will be thrown.</li>
     *         <li>get("some", 1) - a {@link CompactedException} will be thrown.</li>
     *         <li>get("some", 2) - a {@link CompactedException} will be thrown.</li>
     *         <li>get("some", 3) - a {@link CompactedException} will be thrown.</li>
     *     </ul>
     *     </li>
     * </ul>
     *
     * @param key Key.
     * @param revUpperBound Upper bound of revision (inclusive).
     * @throws CompactedException If the requested entry was not found and the {@code revUpperBound} is less than or equal to the last
     *      {@link #setCompactionRevision compacted} one.
     */
    Entry get(byte[] key, long revUpperBound);

    /**
     * Returns the latest version of entries corresponding to the given keys.
     *
     * <p>Never throws {@link CompactedException}.</p>
     *
     * @param keys Not empty keys.
     */
    List<Entry> getAll(List<byte[]> keys);

    /**
     * Returns entries corresponding to the given keys and bounded by the given revision.
     *
     * @param keys Not empty keys.
     * @param revUpperBound Upper bound of revision (inclusive).
     * @throws CompactedException If getting any of the individual entries would have thrown this exception as if
     *      {@link #get(byte[], long)} was used.
     * @see #get(byte[], long)
     */
    List<Entry> getAll(List<byte[]> keys, long revUpperBound);

    /**
     * Saves an index and a term into the storage. These values are used to determine a storage state after restart.
     *
     * @param index Index value.
     * @param term Term value.
     */
    void setIndexAndTerm(long index, long term);

    /**
     * Returns an index and term pair.
     *
     * @return An index and term pair. {@code null} if no {@link #setIndexAndTerm(long, long)} has been called yet on this storage.
     */
    @Nullable IndexWithTerm getIndexWithTerm();

    /**
     * Saves an arbitrary storage configuration as a byte array.
     *
     * @param configuration Configuration bytes.
     * @param lastAppliedIndex Last applied index.
     * @param lastAppliedTerm Last applied term.
     */
    void saveConfiguration(byte[] configuration, long lastAppliedIndex, long lastAppliedTerm);

    /**
     * Returns configuration bytes saved by {@link #saveConfiguration(byte[], long, long)}.
     *
     * @return Configuration bytes saved by {@link #saveConfiguration(byte[], long, long)}. {@code null} if it's
     *         never been called.
     */
    byte @Nullable [] getConfiguration();

    /**
     * Inserts an entry with the given key and given value.
     *
     * @param key The key.
     * @param value The value.
     * @param context Operation's context.
     */
    void put(byte[] key, byte[] value, KeyValueUpdateContext context);

    /**
     * Inserts entries with given keys and given values.
     *
     * @param keys The key list.
     * @param values The values list.
     * @param context Operation's context.
     */
    void putAll(List<byte[]> keys, List<byte[]> values, KeyValueUpdateContext context);

    /**
     * Removes an entry with the given key.
     *
     * @param key The key.
     * @param context Operation's context.
     */
    void remove(byte[] key, KeyValueUpdateContext context);

    /**
     * Remove all entries corresponding to given keys.
     *
     * @param keys The keys list.
     * @param context Operation's context.
     */
    void removeAll(List<byte[]> keys, KeyValueUpdateContext context);

    /**
     * Removes all entries corresponding to given prefix.
     *
     * @param prefix Prefix.
     * @param context Operation's context.
     */
    void removeByPrefix(byte[] prefix, KeyValueUpdateContext context);

    /**
     * Performs {@code success} operation if condition is {@code true}, otherwise performs {@code failure} operations.
     *
     * @param condition Condition.
     * @param success Success operations.
     * @param failure Failure operations.
     * @param context Operation's context.
     * @param commandId Command Id.
     * @return Result of test condition.
     */
    boolean invoke(
            Condition condition,
            List<Operation> success,
            List<Operation> failure,
            KeyValueUpdateContext context,
            CommandId commandId
    );

    /**
     * Invoke, which supports nested conditional statements with left and right branches of execution.
     *
     * @param iif {@link If} statement to invoke
     * @param context Operation's context.
     * @param commandId Command Id.
     * @return execution result
     * @see If
     * @see StatementResult
     */
    StatementResult invoke(If iif, KeyValueUpdateContext context, CommandId commandId);

    /**
     * Returns cursor by latest entries which correspond to the given keys range.
     *
     * <p>Cursor will iterate over a snapshot of keys and their revisions at the time the method was invoked. Also, each entry will be the
     * only one with the most recent revision.</p>
     *
     * <p>Never throws {@link CompactedException} as well as cursor methods.</p>
     *
     * @param keyFrom Start key of range (inclusive).
     * @param keyTo Last key of range (exclusive), {@code null} represents an unbound range.
     */
    Cursor<Entry> range(byte[] keyFrom, byte @Nullable [] keyTo);

    /**
     * Returns cursor by entries which correspond to the given keys range and bounded by revision number.
     *
     * <p>Cursor will iterate over a snapshot of keys and their revisions at the time the method was invoked. And also each record will be
     * one and with a revision less than or equal to the {@code revUpperBound}.</p>
     *
     * <p>Cursor methods never throw {@link CompactedException}.</p>
     *
     * @param keyFrom Start key of range (inclusive).
     * @param keyTo Last key of range (exclusive), {@code null} represents an unbound range.
     * @param revUpperBound Upper bound of revision (inclusive) for each key.
     * @throws CompactedException If the {@code revUpperBound} is less than or equal to the last {@link #setCompactionRevision compacted}
     *      one.
     */
    Cursor<Entry> range(byte[] keyFrom, byte @Nullable [] keyTo, long revUpperBound);

    /**
     * Creates subscription on updates of entries corresponding to the given keys range and starting from the given revision number.
     *
     * @param keyFrom Start key of range (inclusive).
     * @param keyTo Last key of range (exclusive), {@code null} represents an unbound range.
     * @param rev Start revision number.
     */
    void watchRange(byte[] keyFrom, byte @Nullable [] keyTo, long rev, WatchListener listener);

    /**
     * Registers a watch listener for the provided key.
     *
     * @param key Key.
     * @param rev Start revision number.
     * @param listener Listener which will be notified for each update.
     */
    void watchExact(byte[] key, long rev, WatchListener listener);

    /**
     * Registers a watch listener for the provided keys.
     *
     * @param keys Not empty keys.
     * @param rev Start revision number.
     * @param listener Listener which will be notified for each update.
     */
    void watchExact(Collection<byte[]> keys, long rev, WatchListener listener);

    /**
     * Starts all registered watches.
     *
     * <p>Before calling this method, watches will not receive any updates.</p>
     *
     * @param startRevision Revision to start processing updates from.
     * @param callback Watch event handling callback.
     */
    void startWatches(long startRevision, WatchEventHandlingCallback callback);

    /**
     * Unregisters a watch listener.
     */
    void removeWatch(WatchListener listener);

    /**
     * Compacts outdated key versions and removes tombstones of metastorage locally.
     *
     * <p>We do not delete the only and last version of the key unless it is a tombstone.</p>
     *
     * <p>Let's look at some examples, let's say we have the following keys with their versions:</p>
     * <ul>
     *     <li>Key {@code "foo"} with versions that have revisions {@code (1, 3, 5)} - {@code "foo" [1, 3, 5]}.</li>
     *     <li>
     *         Key {@code "bar"} with versions that have revisions {@code (1, 2, 5)} the last revision is a tombstone -
     *         {@code "bar" [1, 2, 5 tomb]}.
     *     </li>
     * </ul>
     *
     * <p>Let's look at examples of invoking the current method and what will be in the storage after:</p>
     * <ul>
     *     <li>
     *         Compaction revision is {@code 1}: {@code "foo" [1, 3, 5]}, {@code "bar" [2, 5 tomb]}.<br>
     *         Explanation:<br>
     *         {@code "foo"[1]} can still be read from {@code get("foo", 2)}.<br>
     *         {@code "bar"[2]} can still be read from {@code get("bar", 2)}.
     *     </li>
     *     <li>
     *         Compaction revision is {@code 2}: {@code "foo" [3, 5]}, {@code "bar" [2, 5 tomb]}.
     *         Explanation:<br>
     *         {@code "foo"[1]} can not be read from {@code get("foo", 3)}, so it is deleted.<br>
     *         {@code "bar"[2]} can still be read from {@code get("bar", 3)}.
     *     </li>
     *     <li>
     *         Compaction revision is {@code 3}: {@code "foo" [3, 5]}, {@code "bar" [2, 5 tomb]}.
     *         Explanation:<br>
     *         {@code "foo"[3]} can still be read from {@code get("foo", 4)}, so it is deleted.<br>
     *         {@code "bar"[2]} can still be read from {@code get("bar", 4)}.
     *     </li>
     *     <li>
     *         Compaction revision is {@code 4}: {@code "foo" [5]}, {@code "bar" [5 tomb]}.
     *         Explanation:<br>
     *         {@code "foo"[3]} can not be read from {@code get("foo", 5)}, so it is deleted.<br>
     *         {@code "bar"[2]} can not be read from {@code get("bar", 5)}, so it is deleted.
     *     </li>
     *     <li>
     *         Compaction revision is {@code 5}: {@code "foo" [5]}.
     *         Explanation:<br>
     *         {@code "foo"[5]} can still be read from {@code get("foo", 6)}, it's the last revision.<br>
     *         {@code "bar"[5]} is a tombstone and its revision is less than or equal compacted revision, so it is deleted.
     *     </li>
     *     <li>
     *         Compaction revision is {@code 6}: {@code "foo" [5]}.
     *         Explanation:<br>
     *         {@code "foo"[5]} can still be read from {@code get("foo", 7)}, it's the last revision.<br>
     *     </li>
     * </ul>
     *
     * <p>Compaction revision is expected to be less than the {@link #revision current storage revision}.</p>
     *
     * <p>Since the node may stop or crash, after restoring the node on its startup we need to run the compaction for the latest known
     * compaction revision.</p>
     *
     * <p>Compaction revision is not updated or saved.</p>
     *
     * @param revision Revision up to which (including) the metastorage keys will be compacted.
     * @throws MetaStorageException If there is an error during the metastorage compaction process.
     * @see #stopCompaction()
     * @see #setCompactionRevision(long)
     * @see #saveCompactionRevision(long, KeyValueUpdateContext)
     */
    void compact(long revision);

    /**
     * Signals the need to stop local metastorage compaction as soon as possible. For example, due to a node stopping.
     *
     * <p>Since compaction of metastorage can take a long time, in order not to be blocked when using it by an external component, it is
     * recommended to invoke this method before stopping the external component.</p>
     */
    void stopCompaction();

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
     * @throws MetaStorageException If there was an error while restoring from a snapshot.
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
     * <p>Requested revision is expected to be less than or equal to the current storage revision.</p>
     *
     * @param revision Revision by which to do a lookup.
     * @return Timestamp corresponding to the revision.
     * @throws CompactedException If the requested revision has been compacted.
     */
    HybridTimestamp timestampByRevision(long revision);

    /**
     * Looks a revision lesser or equal to the timestamp.
     *
     * <p>It is possible to get the revision timestamp using method {@link Entry#timestamp}.</p>
     *
     * @param timestamp Timestamp by which to do a lookup.
     * @return Revision lesser or equal to the timestamp.
     * @throws CompactedException If a revision could not be found by timestamp because it was already compacted.
     */
    long revisionByTimestamp(HybridTimestamp timestamp);

    /**
     * Sets the revisions listener. This is needed only for the recovery, after that listener must be set to {@code null}.
     * {@code null} means that we no longer must be notified of revisions updates for recovery, because recovery is finished.
     */
    void setRecoveryRevisionsListener(@Nullable RecoveryRevisionsListener listener);

    /** Registers a Meta Storage revision update listener. */
    void registerRevisionUpdateListener(RevisionUpdateListener listener);

    /** Unregisters a Meta Storage revision update listener. */
    void unregisterRevisionUpdateListener(RevisionUpdateListener listener);

    /** Registers a metastorage compaction revision update listener. */
    void registerCompactionRevisionUpdateListener(CompactionRevisionUpdateListener listener);

    /** Unregisters a metastorage compaction revision update listener. */
    void unregisterCompactionRevisionUpdateListener(CompactionRevisionUpdateListener listener);

    /** Registers a notification enqueued listener. */
    void registerNotificationEnqueuedListener(NotificationEnqueuedListener listener);

    /** Explicitly notifies revision update listeners. */
    CompletableFuture<Void> notifyRevisionUpdateListenerOnStart(long newRevision);

    /**
     * Advances MetaStorage Safe Time to a new value without creating a new revision.
     *
     * @param context Operation's context.
     */
    void advanceSafeTime(KeyValueUpdateContext context);

    /**
     * Saves the compaction revision to the storage meta.
     *
     * <p>Method only saves the new compaction revision to the meta of storage. After invoking this method the metastorage read methods
     * will <b>not</b> immediately start throwing a {@link CompactedException} if they request a revision less than or equal to the new
     * saved one.</p>
     *
     * <p>Last saved compaction revision will be in the {@link #snapshot snapshot}. When {@link #restoreSnapshot restoring} from a snapshot,
     * compaction revision will be restored after which the metastorage read methods will throw exception {@link CompactedException}.</p>
     *
     * <p>Compaction revision is expected to be less than the {@link #revision current storage revision}.</p>
     *
     * @param revision Compaction revision to save.
     * @param context Operation's context.
     * @throws MetaStorageException If there is an error while saving a compaction revision.
     * @see #setCompactionRevision(long)
     */
    void saveCompactionRevision(long revision, KeyValueUpdateContext context);

    /**
     * Sets the compaction revision, but does not save it, after invoking this method the metastorage read methods will throw a
     * {@link CompactedException} if they request a revision less than or equal to the new one.
     *
     * <p>Compaction revision is expected to be less than the {@link #revision current storage revision}.</p>
     *
     * @param revision Compaction revision.
     * @see #saveCompactionRevision(long, KeyValueUpdateContext)
     */
    void setCompactionRevision(long revision);

    /**
     * Returns the compaction revision that was set or restored from a snapshot, {@code -1} if it has never been updated.
     *
     * @see #setCompactionRevision(long)
     * @see #saveCompactionRevision(long, KeyValueUpdateContext)
     */
    long getCompactionRevision();

    /**
     * Updates the metastorage compaction revision.
     *
     * <p>Algorithm:</p>
     * <ol>
     *     <li>Invokes {@link #saveCompactionRevision}.</li>
     *     <li>If the metastorage is in a recovery state (listener set via {@link #setRecoveryRevisionsListener}), then
     *     {@link #setCompactionRevision} is invoked and the current method is completed.</li>
     *     <li>If the watches have <b>not</b> {@link #startWatches started}, then it will postpone the execution of step 4 until the
     *     watches and the current method is completed.</li>
     *     <li>Otherwise, a new task (A) is added to the WatchEvent queue and the current method is completed.</li>
     *     <li>Task (A) invokes {@link #setCompactionRevision} and invokes {@link CompactionRevisionUpdateListener#onUpdate}.</li>
     * </ol>
     *
     * <p>Compaction revision is expected to be less than the {@link #revision current storage revision}.</p>
     *
     * @param revision Compaction revision to update.
     * @param context Operation's context.
     * @throws MetaStorageException If there is an error while saving a compaction revision.
     */
    void updateCompactionRevision(long revision, KeyValueUpdateContext context);

    /**
     * Returns checksum corresponding to the revision.
     *
     * @param revision Revision.
     * @throws CompactedException If the requested revision has been compacted.
     */
    long checksum(long revision);

    /**
     * Returns information about a checksum and checksummed revisions. Never throws a {@link CompactedException}; if the requested revision
     * is compacted, just returns 0 as checksum (and the requested revision will not fall in
     * {@link ChecksumAndRevisions#minChecksummedRevision()} - {@link ChecksumAndRevisions#maxChecksummedRevision()} interval).
     *
     * @param revision Revision for which to obtain a checksum.
     */
    ChecksumAndRevisions checksumAndRevisions(long revision);

    /**
     * Clears the content of the storage. Should only be called when no one else uses this storage.
     */
    void clear();

    /**
     * Returns current metastorage revisions.
     *
     * @see #revision()
     * @see #getCompactionRevision()
     */
    Revisions revisions();

    /**
     * Flushes current state of the data or <i>the state from the nearest future</i> to the storage.
     *
     * @return Future that's completed when flushing of the data is completed.
     */
    CompletableFuture<Void> flush();
}
