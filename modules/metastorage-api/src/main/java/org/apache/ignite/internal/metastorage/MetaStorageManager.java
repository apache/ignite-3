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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
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
     * Retrieves an entry for the given key.
     */
    CompletableFuture<Entry> get(ByteArray key);

    /**
     * Retrieves an entry for the given key and the revision upper bound.
     */
    CompletableFuture<Entry> get(ByteArray key, long revUpperBound);

    /**
     * Returns all entries corresponding to the given key and bounded by given revisions.
     * All these entries are ordered by revisions and have the same key.
     * The lower bound and the upper bound are inclusive.
     *
     * <p>This method doesn't wait for the storage's revision to become greater or equal to the revUpperBound parameter, so it is
     * up to user to wait for the appropriate time to call this method.
     * TODO: IGNITE-19735 move this method to another interface for interaction with local KeyValueStorage.
     *
     * @param key The key.
     * @param revLowerBound The lower bound of revision.
     * @param revUpperBound The upper bound of revision.
     * @return Entries corresponding to the given key.
     */
    @Deprecated
    List<Entry> getLocally(byte[] key, long revLowerBound, long revUpperBound);

    /**
     * Returns an entry by the given key and bounded by the given revision. The entry is obtained
     * from the local storage.
     *
     * <p>This method doesn't wait for the storage's revision to become greater or equal to the revUpperBound parameter, so it is
     * up to user to wait for the appropriate time to call this method.
     *
     * @param key The key.
     * @param revUpperBound The upper bound of revision.
     * @return Value corresponding to the given key.
     */
    Entry getLocally(ByteArray key, long revUpperBound);

    /**
     * Returns cursor by entries which correspond to the given keys range and bounded by revision number. The entries in the cursor
     * are obtained from the local storage.
     *
     * <p>This method doesn't wait for the storage's revision to become greater or equal to the revUpperBound parameter, so it is
     * up to user to wait for the appropriate time to call this method.
     *
     * @param startKey Start key of range (inclusive).
     * @param endKey Last key of range (exclusive).
     * @param revUpperBound Upper bound of revision.
     * @return Cursor by entries which correspond to the given keys range.
     */
    Cursor<Entry> getLocally(ByteArray startKey, ByteArray endKey, long revUpperBound);

    /**
     * Returns cursor by entries which correspond to the given key prefix and bounded by revision number. The entries in the cursor
     * are obtained from the local storage.
     *
     * <p>This method doesn't wait for the storage's revision to become greater or equal to the revUpperBound parameter, so it is
     * up to user to wait for the appropriate time to call this method.
     *
     * @param keyPrefix Key prefix.
     * @param revUpperBound Upper bound of revision.
     * @return Cursor by entries which correspond to the given key prefix.
     */
    Cursor<Entry> prefixLocally(ByteArray keyPrefix, long revUpperBound);

    /**
     * Looks up a timestamp by a revision. This should only be invoked if it is guaranteed that the
     * revision is available in the local storage. This method always operates locally.
     *
     * @param revision Revision by which to do a lookup.
     * @return Timestamp corresponding to the revision.
     */
    HybridTimestamp timestampByRevision(long revision);

    /**
     * Retrieves entries for given keys.
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
     * Retrieves entries for the given key prefix in lexicographic order. Shortcut for {@link #prefix(ByteArray, long)} where
     * {@code revUpperBound = LATEST_REVISION}.
     *
     * @param keyPrefix Prefix of the key to retrieve the entries. Couldn't be {@code null}.
     * @return Publisher that will provide entries corresponding to the given prefix. This Publisher may also fail (by calling
     *     {@link Subscriber#onError}) with one of the following exceptions:
     *     <ul>
     *         <li>{@link OperationTimeoutException} - if the operation is timed out;</li>
     *         <li>{@link CompactedException} - if the desired revisions are removed from the storage due to a compaction;</li>
     *         <li>{@link NodeStoppingException} - if this node has been stopped.</li>
     *     </ul>
     */
    Publisher<Entry> prefix(ByteArray keyPrefix);

    /**
     * Retrieves entries for the given key prefix in lexicographic order. Entries will be filtered out by upper bound of given revision
     * number.
     *
     * @param keyPrefix Prefix of the key to retrieve the entries. Couldn't be {@code null}.
     * @param revUpperBound The upper bound for entry revision or {@link MetaStorageManager#LATEST_REVISION} for no revision bound.
     * @return Publisher that will provide entries corresponding to the given prefix and revision. This Publisher may also fail (by calling
     *     {@link Subscriber#onError}) with one of the following exceptions:
     *     <ul>
     *         <li>{@link OperationTimeoutException} - if the operation is timed out;</li>
     *         <li>{@link CompactedException} - if the desired revisions are removed from the storage due to a compaction;</li>
     *         <li>{@link NodeStoppingException} - if this node has been stopped.</li>
     *     </ul>
     */
    Publisher<Entry> prefix(ByteArray keyPrefix, long revUpperBound);

    /**
     * Retrieves entries for the given key range in lexicographic order.
     *
     * @param keyFrom Range lower bound (inclusive).
     * @param keyTo Range upper bound (exclusive), {@code null} represents an unbound range.
     * @return Publisher that will provide entries corresponding to the given range. This Publisher may also fail (by calling
     *     {@link Subscriber#onError}) with one of the following exceptions:
     *     <ul>
     *         <li>{@link OperationTimeoutException} - if the operation is timed out;</li>
     *         <li>{@link CompactedException} - if the desired revisions are removed from the storage due to a compaction;</li>
     *         <li>{@link NodeStoppingException} - if this node has been stopped.</li>
     *     </ul>
     */
    Publisher<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo);

    /**
     * Invoke with single success/failure operation.
     */
    CompletableFuture<Boolean> invoke(Condition cond, Operation success, Operation failure);

    /**
     * Updates an entry for the given key conditionally.
     */
    CompletableFuture<Boolean> invoke(Condition cond, Collection<Operation> success, Collection<Operation> failure);

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
     * The value of the future is the revision which must be used for state recovery by other components.
     */
    CompletableFuture<Long> recoveryFinishedFuture();

    /** Registers a Meta Storage revision update listener. */
    void registerRevisionUpdateListener(RevisionUpdateListener listener);

    /** Unregisters a Meta Storage revision update listener. */
    void unregisterRevisionUpdateListener(RevisionUpdateListener listener);
}
