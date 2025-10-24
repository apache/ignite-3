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

package org.apache.ignite.internal.metastorage;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.exceptions.OperationTimeoutException;
import org.apache.ignite.internal.metastorage.server.time.ClusterTime;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Manager that provides operations on the Meta Storage.
 */
public interface MetaStorageManager extends IgniteComponent {
    /**
     * Special revision value indicating that the most recent revision at the time of a request should be used.
     */
    int LATEST_REVISION = -1;

    /**
     * Returns the current <em>applied revision</em> of the Meta Storage, that is, the most recent revision of updates that have been
     * processed by all Watches on this node.
     */
    long appliedRevision();

    /**
     * Returns a future which will hold the current revision of the metastorage leader.
     */
    CompletableFuture<Long> currentRevision();

    /**
     * Returns a future of getting the latest version of an entry by key from the metastorage leader.
     *
     * <p>Never completes with a {@link CompactedException}.</p>
     *
     * <p>Future may complete with {@link NodeStoppingException} if the node is in the process of stopping.</p>
     *
     * @param key Key.
     */
    CompletableFuture<Entry> get(ByteArray key);

    /**
     * Returns a future of getting an entry for the given key and the revision upper bound from the metastorage leader.
     *
     * <p>Future may complete with exceptions:</p>
     * <ul>
     *     <li>{@link NodeStoppingException} - if the node is in the process of stopping.</li>
     *     <li>{@link CompactedException} - if the requested entry was not found and the {@code revUpperBound} is less than or equal to the
     *     last compacted one. For examples see {@link #getLocally(ByteArray, long)}.</li>
     * </ul>
     *
     * @param key Key.
     * @param revUpperBound Upper bound of revision (inclusive).
     * @see #getLocally(ByteArray, long)
     */
    CompletableFuture<Entry> get(ByteArray key, long revUpperBound);

    /**
     * Returns a latest entry for the given key locally. See also {@link #getLocally(ByteArray, long)}.
     *
     * @param key Key.
     * @return Entry.
     */
    Entry getLocally(ByteArray key);

    /**
     * Returns an entry for the given key and the revision upper bound locally.
     *
     * <p>This method doesn't wait for the storage's revision to become greater or equal to the revUpperBound parameter, so it is
     * up to user to wait for the appropriate time to call this method.
     *
     * <p>Let's consider examples of the work of the method and compaction of the metastorage. Let's assume that we have keys with revisions
     * "foo" [1, 2] and "bar" [1, 2 (tombstone)], and the key "some" has never been in the metastorage.</p>
     * <ul>
     *     <li>Compaction revision is {@code 1}.
     *     <ul>
     *         <li>getLocally("foo", 1) - a {@link CompactedException} will be thrown.</li>
     *         <li>getLocally("foo", 2) - will return a single value with revision 2.</li>
     *         <li>getLocally("foo", 3) - will return a single value with revision 2.</li>
     *         <li>getLocally("bar", 1) - a {@link CompactedException} will be thrown.</li>
     *         <li>getLocally("bar", 2) - will return a single value with revision 2.</li>
     *         <li>getLocally("bar", 3) - will return a single value with revision 2.</li>
     *         <li>getLocally("some", 1) - a {@link CompactedException} will be thrown.</li>
     *         <li>getLocally("some", 2) - will return an empty value.</li>
     *         <li>getLocally("some", 3) - will return an empty value.</li>
     *     </ul>
     *     </li>
     *     <li>Compaction revision is {@code 2}.
     *     <ul>
     *         <li>getLocally("foo", 1) - a {@link CompactedException} will be thrown.</li>
     *         <li>getLocally("foo", 2) - will return a single value with revision 2.</li>
     *         <li>getLocally("foo", 3) - will return a single value with revision 2.</li>
     *         <li>getLocally("bar", 1) - a {@link CompactedException} will be thrown.</li>
     *         <li>getLocally("bar", 2) - a {@link CompactedException} will be thrown.</li>
     *         <li>getLocally("bar", 3) - will return a single value with revision 2.</li>
     *         <li>getLocally("some", 1) - a {@link CompactedException} will be thrown.</li>
     *         <li>getLocally("some", 2) - a {@link CompactedException} will be thrown.</li>
     *         <li>getLocally("some", 3) - will return an empty value.</li>
     *     </ul>
     *     </li>
     *     <li>Compaction revision is {@code 3}.
     *     <ul>
     *         <li>getLocally("foo", 1) - a {@link CompactedException} will be thrown.</li>
     *         <li>getLocally("foo", 2) - will return a single value with revision 2.</li>
     *         <li>getLocally("foo", 3) - will return a single value with revision 2.</li>
     *         <li>getLocally("bar", 1) - a {@link CompactedException} will be thrown.</li>
     *         <li>getLocally("bar", 2) - a {@link CompactedException} will be thrown.</li>
     *         <li>getLocally("bar", 3) - a {@link CompactedException} will be thrown.</li>
     *         <li>getLocally("some", 1) - a {@link CompactedException} will be thrown.</li>
     *         <li>getLocally("some", 2) - a {@link CompactedException} will be thrown.</li>
     *         <li>getLocally("some", 3) - a {@link CompactedException} will be thrown.</li>
     *     </ul>
     *     </li>
     * </ul>
     *
     * @param key Key.
     * @param revUpperBound Upper bound of revision (inclusive).
     * @throws IgniteInternalException with cause {@link NodeStoppingException} if the node is in the process of stopping.
     * @throws CompactedException If the requested entry was not found and the {@code revUpperBound} is less than or equal to the last
     *      compacted one.
     */
    Entry getLocally(ByteArray key, long revUpperBound);

    /**
     * Returns cursor by entries which correspond to the given keys range and bounded by revision number locally.
     *
     * <p>Cursor will iterate over a snapshot of keys and their revisions at the time the method was invoked.</p>
     *
     * <p>Cursor methods never throw {@link CompactedException}.</p>
     *
     * <p>This method doesn't wait for the storage's revision to become greater or equal to the revUpperBound parameter, so it is
     * up to user to wait for the appropriate time to call this method.</p>
     *
     * @param startKey Start key of range (inclusive).
     * @param endKey Last key of range (exclusive), {@code null} represents an unbound range.
     * @param revUpperBound Upper bound of revision (inclusive) for each key.
     * @throws IgniteInternalException with cause {@link NodeStoppingException} if the node is in the process of stopping.
     * @throws CompactedException If the {@code revUpperBound} is less than or equal to the last compacted one.
     */
    Cursor<Entry> getLocally(ByteArray startKey, @Nullable ByteArray endKey, long revUpperBound);

    /**
     * Returns a future of getting latest entries corresponding to the given keys from the metastorage locally.
     *
     * @param keys List of keys (must not be empty).
     */
    List<Entry> getAllLocally(List<ByteArray> keys);

    /**
     * Returns cursor by entries which correspond to the given key prefix and bounded by revision number locally.
     *
     * <p>Cursor will iterate over a snapshot of keys and their revisions at the time the method was invoked.</p>
     *
     * <p>Cursor methods never throw {@link CompactedException}.</p>
     *
     * <p>This method doesn't wait for the storage's revision to become greater or equal to the revUpperBound parameter, so it is
     * up to user to wait for the appropriate time to call this method.</p>
     *
     * @param keyPrefix Key prefix.
     * @param revUpperBound Upper bound of revision (inclusive) for each key.
     * @throws IgniteInternalException with cause {@link NodeStoppingException} if the node is in the process of stopping.
     * @throws CompactedException If the {@code revUpperBound} is less than or equal to the last compacted one.
     */
    Cursor<Entry> prefixLocally(ByteArray keyPrefix, long revUpperBound);

    /**
     * Looks up a timestamp by a revision. This should only be invoked if it is guaranteed that the
     * revision is available in the local storage. This method always operates locally.
     *
     * <p>Requested revision is expected to be less than or equal to the current metastorage revision.</p>
     *
     * @param revision Revision by which to do a lookup.
     * @return Timestamp corresponding to the revision.
     * @throws IgniteInternalException with cause {@link NodeStoppingException} if the node is in the process of stopping.
     * @throws CompactedException If the requested revision has been compacted.
     */
    HybridTimestamp timestampByRevisionLocally(long revision);

    /**
     * Returns a future of getting the latest version of entries corresponding to the given keys from the metastorage leader.
     *
     * <p>Never completes with a {@link CompactedException}.</p>
     *
     * <p>Future may complete with {@link NodeStoppingException} if the node is in the process of stopping.</p>
     *
     * @param keys Set of keys (must not be empty).
     */
    CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys);

    /**
     * Inserts or updates an entry with the given key and the given value.
     */
    CompletableFuture<Void> put(ByteArray key, byte[] val);

    /**
     * Inserts or updates entries with given keys and given values.
     */
    CompletableFuture<Void> putAll(Map<ByteArray, byte[]> vals);

    /**
     * Removes an entry for the given key.
     */
    CompletableFuture<Void> remove(ByteArray key);

    /**
     * Removes entries for given keys.
     */
    CompletableFuture<Void> removeAll(Set<ByteArray> keys);

    /**
     * Removes entries by the given prefix.
     */
    CompletableFuture<Void> removeByPrefix(ByteArray prefix);

    /**
     * Returns a publisher for getting the latest version of an entries for the given key prefix from the metastorage leader.
     *
     * <p>Never fail with a {@link CompactedException}.</p>
     *
     * <p>Publisher may fail (by calling {@link Subscriber#onError}) with one of the following exceptions:</p>
     * <ul>
     *     <li>{@link NodeStoppingException} - if the node is in the process of stopping.</li>
     *     <li>{@link OperationTimeoutException} - if the operation is timed out.</li>
     * </ul>
     *
     * @param keyPrefix Key prefix.
     */
    Publisher<Entry> prefix(ByteArray keyPrefix);

    /**
     * Returns a publisher for getting an entries for the given key prefix and the revision upper bound from the metastorage leader.
     *
     * <p>Publisher may fail (by calling {@link Subscriber#onError}) with one of the following exceptions:</p>
     * <ul>
     *     <li>{@link NodeStoppingException} - if the node is in the process of stopping.</li>
     *     <li>{@link OperationTimeoutException} - if the operation is timed out.</li>
     *     <li>{@link CompactedException} - if the {@code revUpperBound} is less than or equal to the last compacted one on metastorage
     *     leader, can occur while processing any batch of entries.</li>
     * </ul>
     *
     * @param keyPrefix Key prefix.
     * @param revUpperBound Upper bound of revision (inclusive) for each key.
     */
    Publisher<Entry> prefix(ByteArray keyPrefix, long revUpperBound);

    /**
     * Returns a publisher for getting the latest version of an entries for the given keys range from the metastorage leader.
     *
     * <p>Never fail with a {@link CompactedException}.</p>
     *
     * <p>Publisher may fail (by calling {@link Subscriber#onError}) with one of the following exceptions:</p>
     * <ul>
     *     <li>{@link NodeStoppingException} - if the node is in the process of stopping.</li>
     *     <li>{@link OperationTimeoutException} - if the operation is timed out.</li>
     * </ul>
     *
     * @param keyFrom Start key of range (inclusive).
     * @param keyTo Last key of range (exclusive), {@code null} represents an unbound range.
     */
    Publisher<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo);

    /**
     * Invoke with single success/failure operation.
     */
    CompletableFuture<Boolean> invoke(Condition cond, Operation success, Operation failure);

    /**
     * Updates an entry for the given key conditionally.
     */
    CompletableFuture<Boolean> invoke(Condition cond, List<Operation> success, List<Operation> failure);

    /**
     * Invoke, which supports nested conditional statements.
     */
    CompletableFuture<StatementResult> invoke(Iif iif);

    /**
     * Registers a watch listener by a key prefix.
     *
     * @param key Prefix to listen to.
     * @param lsnr Listener which will be notified for each update.
     */
    void registerPrefixWatch(ByteArray key, WatchListener lsnr);

    /**
     * Registers a watch listener for the provided key.
     *
     * @param key Meta Storage key.
     * @param listener Listener which will be notified for each update.
     */
    void registerExactWatch(ByteArray key, WatchListener listener);

    /**
     * Registers a watch listener by a key range.
     *
     * @param keyFrom Start of the range (inclusive).
     * @param keyTo End of the range (exclusive) or {@code null} if the range doesn't have an upper bound.
     * @param listener Listener which will be notified for each update.
     */
    void registerRangeWatch(ByteArray keyFrom, @Nullable ByteArray keyTo, WatchListener listener);

    /**
     * Unregisters a watch listener.
     */
    void unregisterWatch(WatchListener lsnr);

    /**
     * Starts all registered watches.
     *
     * <p>Should be called after all Ignite components have registered required watches and they are ready to process Meta Storage events.
     *
     * @return Future which completes when Meta storage manager is started and deploying watches is finished.
     */
    CompletableFuture<Void> deployWatches();

    /**
     * Returns cluster time with a hybrid clock instance and access to safe time.
     *
     * @return Cluster time.
     */
    ClusterTime clusterTime();

    /**
     * Returns a future which completes when MetaStorage manager finished local recovery.
     * The value of the future is the revisions which must be used for state recovery by other components.
     */
    CompletableFuture<Revisions> recoveryFinishedFuture();

    /** Registers a Meta Storage revision update listener. */
    void registerRevisionUpdateListener(RevisionUpdateListener listener);

    /** Unregisters a Meta Storage revision update listener. */
    void unregisterRevisionUpdateListener(RevisionUpdateListener listener);

    /** Registers a Meta Storage compaction revision update listener. */
    void registerCompactionRevisionUpdateListener(CompactionRevisionUpdateListener listener);

    /** Unregisters a Meta Storage compaction revision update listener. */
    void unregisterCompactionRevisionUpdateListener(CompactionRevisionUpdateListener listener);

    /**
     * Returns the local compaction revision that was set or restored from a metastorage snapshot, {@code -1} if it has never been updated.
     *
     * @throws IgniteInternalException with cause {@link NodeStoppingException} if the node is in the process of stopping.
     */
    long getCompactionRevisionLocally();
}
