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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.exceptions.OperationTimeoutException;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.Nullable;

/**
 * Manager that provides operations on the Meta Storage.
 */
public interface MetaStorageManager extends IgniteComponent {
    /**
     * Returns the current <em>applied revision</em> of the Meta Storage, that is, the most recent revision of updates that have been
     * applied on this node.
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
     * Retrieves entries for given keys.
     */
    CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys);

    /**
     * Retrieves entries for the given key prefix in lexicographic order. Shortcut for {@link #prefix(ByteArray, long)} where
     * {@code revUpperBound == -1}.
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
     * @param revUpperBound The upper bound for entry revision. {@code -1} means latest revision.
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
     */
    void deployWatches() throws NodeStoppingException;
}
