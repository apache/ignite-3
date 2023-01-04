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
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.If;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.exceptions.OperationTimeoutException;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.Nullable;

/**
 * Manager that provides operations on the Meta Storage.
 */
public interface MetaStorageManager extends IgniteComponent {
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
     * @return Cursor built upon entries corresponding to the given range and revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    Cursor<Entry> prefix(ByteArray keyPrefix) throws NodeStoppingException;

    /**
     * Retrieves entries for the given key prefix in lexicographic order. Entries will be filtered out by upper bound of given revision
     * number.
     *
     * <p>Prefix query is a synonym of the range query {@code range(prefixKey, nextKey(prefixKey))}.
     *
     * @param keyPrefix Prefix of the key to retrieve the entries. Couldn't be {@code null}.
     * @param revUpperBound The upper bound for entry revision. {@code -1} means latest revision.
     * @return Cursor built upon entries corresponding to the given range and revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    Cursor<Entry> prefix(ByteArray keyPrefix, long revUpperBound) throws NodeStoppingException;

    /**
     * Retrieves entries for the given key range in lexicographic order.
     */
    Cursor<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo) throws NodeStoppingException;

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
    CompletableFuture<StatementResult> invoke(If iif);

    /**
     * Register watch listener by key prefix.
     *
     * @param key Prefix to listen.
     * @param lsnr Listener which will be notified for each update.
     * @return Subscription identifier. Could be used in {@link #unregisterWatch} method in order to cancel subscription
     */
    CompletableFuture<Long> registerWatchByPrefix(ByteArray key, WatchListener lsnr);

    /**
     * Unregister watch listener by id.
     *
     * @param id of watch to unregister.
     * @return future, which will be completed when unregister finished.
     */
    CompletableFuture<Void> unregisterWatch(long id);

    /**
     * Deploy all registered watches.
     */
    void deployWatches() throws NodeStoppingException;
}
