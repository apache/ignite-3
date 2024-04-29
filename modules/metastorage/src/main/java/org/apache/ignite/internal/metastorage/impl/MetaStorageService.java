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

package org.apache.ignite.internal.metastorage.impl;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.exceptions.OperationTimeoutException;
import org.jetbrains.annotations.Nullable;

/**
 * Defines interface for access to a meta storage service.
 */
public interface MetaStorageService extends ManuallyCloseable {
    /**
     * Retrieves an entry for the given key.
     *
     * @param key Key. Couldn't be {@code null}.
     * @return An entry for the given key. Couldn't be {@code null}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    CompletableFuture<Entry> get(ByteArray key);

    /**
     * Retrieves an entry for the given key and the revision upper bound.
     *
     * @param key The key. Couldn't be {@code null}.
     * @param revUpperBound The upper bound for entry revisions. Must be positive.
     * @return An entry for the given key and maximum revision limited by {@code revUpperBound}. Couldn't be {@code null}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction. Will be thrown on getting
     *      future result.
     * @see ByteArray
     * @see Entry
     */
    CompletableFuture<Entry> get(ByteArray key, long revUpperBound);

    /**
     * Retrieves entries for given keys.
     *
     * @param keys The set of keys. Couldn't be {@code null} or empty. Set elements couldn't be {@code null}.
     * @return A map of entries for given keys. Couldn't be {@code null}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys);

    /**
     * Retrieves entries for given keys and the revision upper bound.
     *
     * @param keys The set of keys. Couldn't be {@code null} or empty. Set elements couldn't be {@code null}.
     * @param revUpperBound The upper bound for entry revisions. Must be positive.
     * @return A map of entries for given keys and maximum revision limited by {@code revUpperBound}. Couldn't be {@code null}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction. Will be thrown on getting
     *      future result.
     * @see ByteArray
     * @see Entry
     */
    CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys, long revUpperBound);

    /**
     * Inserts or updates an entry with the given key and the given value.
     *
     * @param key The key. Couldn't be {@code null}.
     * @param value The value. Couldn't be {@code null}.
     * @return Completed future.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    CompletableFuture<Void> put(ByteArray key, byte[] value);

    /**
     * Inserts or updates entries with given keys and given values.
     *
     * @param vals The map of keys and corresponding values. Couldn't be {@code null} or empty.
     * @return Completed future.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    CompletableFuture<Void> putAll(Map<ByteArray, byte[]> vals);

    /**
     * Removes an entry for the given key.
     *
     * @param key The key. Couldn't be {@code null}.
     * @return Completed future.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    CompletableFuture<Void> remove(ByteArray key);

    /**
     * Removes entries for given keys.
     *
     * @param keys The keys set. Couldn't be {@code null} or empty.
     * @return Completed future.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    CompletableFuture<Void> removeAll(Set<ByteArray> keys);

    /**
     * Updates an entry for the given key conditionally.
     *
     * <p>Conditional update could be treated as <i>if(condition)-then(success)-else(failure)</i> expression.</p>
     *
     * @param condition The condition.
     * @param success The update which will be applied in case of condition evaluation yields {@code true}.
     * @param failure The update which will be applied in case of condition evaluation yields {@code false}.
     * @return Future result {@code true} if {@code success} update was applied, otherwise {@code false}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     * @see Condition
     * @see Operation
     */
    CompletableFuture<Boolean> invoke(Condition condition, Operation success, Operation failure);

    /**
     * Updates an entry for the given key conditionally.
     *
     * <p>Conditional update could be treated as <i>if(condition)-then(success)-else(failure)</i> expression.</p>
     *
     * @param condition The condition.
     * @param success The updates which will be applied in case of condition evaluation yields {@code true}.
     * @param failure The updates which will be applied in case of condition evaluation yields {@code false}.
     * @return Future result {@code true} if {@code success} update was applied, otherwise {@code false}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     * @see Condition
     * @see Operation
     */
    CompletableFuture<Boolean> invoke(Condition condition, Collection<Operation> success, Collection<Operation> failure);

    /**
     * Invoke, which supports nested conditional statements. For detailed docs about construction of new if statement, look at {@link Iif}
     * javadocs.
     *
     * @param iif {@link Iif} statement to invoke
     * @return execution result
     * @see Iif
     * @see StatementResult
     */
    CompletableFuture<StatementResult> invoke(Iif iif);

    /**
     * Retrieves entries for the given key range in lexicographic order. Entries will be filtered out by upper bound of given revision
     * number.
     *
     * @param keyFrom Start key of range (inclusive). Couldn't be {@code null}.
     * @param keyTo End key of range (exclusive). {@code null} represents an unbound range.
     * @param revUpperBound The upper bound for entry revision. {@code -1} means latest revision.
     * @return  Publisher that will provide entries corresponding to the given range and revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    Publisher<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo, long revUpperBound);

    /**
     * Retrieves entries for the given key range in lexicographic order. Entries will be filtered out by upper bound of given revision
     * number.
     *
     * @param keyFrom Start key of range (inclusive). Couldn't be {@code null}.
     * @param keyTo End key of range (exclusive). {@code null} represents an unbound range.
     * @param revUpperBound The upper bound for entry revision. {@code -1} means latest revision.
     * @param includeTombstones Whether to include tombstone entries.
     * @return  Publisher that will provide entries corresponding to the given range and revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    Publisher<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo, long revUpperBound, boolean includeTombstones);

    /**
     * Retrieves entries for the given key range in lexicographic order. Short cut for {@link #range(ByteArray, ByteArray, long)} where
     * {@code revUpperBound == -1}.
     *
     * @param keyFrom Start key of range (inclusive). Couldn't be {@code null}.
     * @param keyTo End key of range (exclusive). {@code null} represents an unbound range.
     * @return  Publisher that will provide entries corresponding to the given range.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    Publisher<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo);

    /**
     * Retrieves entries for the given key range in lexicographic order. Short cut for {@link #range(ByteArray, ByteArray, long, boolean)}
     * where {@code revUpperBound == -1}.
     *
     * @param keyFrom Start key of range (inclusive). Couldn't be {@code null}.
     * @param keyTo End key of range (exclusive). {@code null} represents an unbound range.
     * @param includeTombstones Whether to include tombstone entries.
     * @return Publisher that will provide entries corresponding to the given range.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    Publisher<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo, boolean includeTombstones);

    /**
     * Retrieves entries for keys starting with the given prefix in lexicographic order.
     *
     * @param prefix Key prefix.
     * @param revUpperBound The upper bound for entry revision. {@code -1} means latest revision.
     * @return Publisher that will provide entries corresponding to the given prefix and revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     */
    Publisher<Entry> prefix(ByteArray prefix, long revUpperBound);

    /**
     * Compacts meta storage (removes all tombstone entries and old entries except of entries with latest revision).
     *
     * @return Completed future. Couldn't be {@code null}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     */
    CompletableFuture<Void> compact();

    /**
     * Returns a future which will hold current revision of the metastorage leader.
     */
    CompletableFuture<Long> currentRevision();
}
