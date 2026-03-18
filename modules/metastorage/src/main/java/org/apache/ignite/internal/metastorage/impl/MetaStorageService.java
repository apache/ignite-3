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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.command.response.ChecksumInfo;
import org.apache.ignite.internal.metastorage.command.response.RevisionsInfo;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.exceptions.OperationTimeoutException;
import org.jetbrains.annotations.Nullable;

/**
 * Defines interface for access to a meta storage service.
 *
 * <p>All methods accept a {@code timeoutMillis} parameter with the following semantics:
 * <ul>
 *     <li>{@code 0} - single attempt without retries.</li>
 *     <li>{@code Long.MAX_VALUE} - infinite wait.</li>
 *     <li>negative values - treated as infinite for compatibility.</li>
 *     <li>positive values - bounded wait up to the specified timeout.</li>
 * </ul>
 */
public interface MetaStorageService extends ManuallyCloseable {
    /**
     * Retrieves an entry for the given key.
     *
     * @param key Key. Couldn't be {@code null}.
     * @param timeoutMillis Timeout in milliseconds.
     * @return An entry for the given key. Couldn't be {@code null}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    CompletableFuture<Entry> get(ByteArray key, long timeoutMillis);

    /**
     * Retrieves an entry for the given key and the revision upper bound.
     *
     * @param key The key. Couldn't be {@code null}.
     * @param revUpperBound The upper bound for entry revisions. Must be positive.
     * @param timeoutMillis Timeout in milliseconds.
     * @return An entry for the given key and maximum revision limited by {@code revUpperBound}. Couldn't be {@code null}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction. Will be thrown on getting
     *      future result.
     * @see ByteArray
     * @see Entry
     */
    CompletableFuture<Entry> get(ByteArray key, long revUpperBound, long timeoutMillis);

    /**
     * Retrieves entries for given keys.
     *
     * @param keys The set of keys. Couldn't be {@code null} or empty. Set elements couldn't be {@code null}.
     * @param timeoutMillis Timeout in milliseconds.
     * @return A map of entries for given keys. Couldn't be {@code null}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys, long timeoutMillis);

    /**
     * Retrieves entries for given keys and the revision upper bound.
     *
     * @param keys The set of keys. Couldn't be {@code null} or empty. Set elements couldn't be {@code null}.
     * @param revUpperBound The upper bound for entry revisions. Must be positive.
     * @param timeoutMillis Timeout in milliseconds.
     * @return A map of entries for given keys and maximum revision limited by {@code revUpperBound}. Couldn't be {@code null}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction. Will be thrown on getting
     *      future result.
     * @see ByteArray
     * @see Entry
     */
    CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys, long revUpperBound, long timeoutMillis);

    /**
     * Inserts or updates an entry with the given key and the given value.
     *
     * @param key The key. Couldn't be {@code null}.
     * @param value The value. Couldn't be {@code null}.
     * @param timeoutMillis Timeout in milliseconds.
     * @return Completed future.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    CompletableFuture<Void> put(ByteArray key, byte[] value, long timeoutMillis);

    /**
     * Inserts or updates entries with given keys and given values.
     *
     * @param vals The map of keys and corresponding values. Couldn't be {@code null} or empty.
     * @param timeoutMillis Timeout in milliseconds.
     * @return Completed future.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    CompletableFuture<Void> putAll(Map<ByteArray, byte[]> vals, long timeoutMillis);

    /**
     * Removes an entry for the given key.
     *
     * @param key The key. Couldn't be {@code null}.
     * @param timeoutMillis Timeout in milliseconds.
     * @return Completed future.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    CompletableFuture<Void> remove(ByteArray key, long timeoutMillis);

    /**
     * Removes entries for given keys.
     *
     * @param keys The keys set. Couldn't be {@code null} or empty.
     * @param timeoutMillis Timeout in milliseconds.
     * @return Completed future.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    CompletableFuture<Void> removeAll(Set<ByteArray> keys, long timeoutMillis);

    /**
     * Removes entries by given prefix.
     *
     * @param prefix Prefix to remove keys by. Couldn't be {@code null}.
     * @param timeoutMillis Timeout in milliseconds.
     * @return Future that completes successfully when keys with given prefix are deleted or with {@link OperationTimeoutException}.
     * @see ByteArray
     * @see Entry
     */
    CompletableFuture<Void> removeByPrefix(ByteArray prefix, long timeoutMillis);

    /**
     * Updates an entry for the given key conditionally.
     *
     * <p>Conditional update could be treated as <i>if(condition)-then(success)-else(failure)</i> expression.</p>
     *
     * @param condition The condition.
     * @param success The update which will be applied in case of condition evaluation yields {@code true}.
     * @param failure The update which will be applied in case of condition evaluation yields {@code false}.
     * @param timeoutMillis Timeout in milliseconds.
     * @return Future result {@code true} if {@code success} update was applied, otherwise {@code false}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     * @see Condition
     * @see Operation
     */
    CompletableFuture<Boolean> invoke(Condition condition, Operation success, Operation failure, long timeoutMillis);

    /**
     * Updates an entry for the given key conditionally.
     *
     * <p>Conditional update could be treated as <i>if(condition)-then(success)-else(failure)</i> expression.</p>
     *
     * @param condition The condition.
     * @param success The updates which will be applied in case of condition evaluation yields {@code true}.
     * @param failure The updates which will be applied in case of condition evaluation yields {@code false}.
     * @param timeoutMillis Timeout in milliseconds.
     * @return Future result {@code true} if {@code success} update was applied, otherwise {@code false}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     * @see Condition
     * @see Operation
     */
    CompletableFuture<Boolean> invoke(Condition condition, List<Operation> success, List<Operation> failure, long timeoutMillis);

    /**
     * Invoke, which supports nested conditional statements. For detailed docs about construction of new if statement, look at {@link Iif}
     * javadocs.
     *
     * @param iif {@link Iif} statement to invoke
     * @param timeoutMillis Timeout in milliseconds.
     * @return execution result
     * @see Iif
     * @see StatementResult
     */
    CompletableFuture<StatementResult> invoke(Iif iif, long timeoutMillis);

    /**
     * Retrieves entries for the given key range in lexicographic order. Entries will be filtered out by upper bound of given revision
     * number.
     *
     * @param keyFrom Start key of range (inclusive). Couldn't be {@code null}.
     * @param keyTo End key of range (exclusive). {@code null} represents an unbound range.
     * @param revUpperBound The upper bound for entry revision. {@code -1} means latest revision.
     * @param timeoutMillis Timeout in milliseconds.
     * @return  Publisher that will provide entries corresponding to the given range and revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    Publisher<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo, long revUpperBound, long timeoutMillis);

    /**
     * Retrieves entries for the given key range in lexicographic order. Entries will be filtered out by upper bound of given revision
     * number.
     *
     * @param keyFrom Start key of range (inclusive). Couldn't be {@code null}.
     * @param keyTo End key of range (exclusive). {@code null} represents an unbound range.
     * @param revUpperBound The upper bound for entry revision. {@code -1} means latest revision.
     * @param includeTombstones Whether to include tombstone entries.
     * @param timeoutMillis Timeout in milliseconds.
     * @return  Publisher that will provide entries corresponding to the given range and revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    Publisher<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo, long revUpperBound, boolean includeTombstones,
            long timeoutMillis);

    /**
     * Retrieves entries for the given key range in lexicographic order. Short cut for
     * {@link #range(ByteArray, ByteArray, long, long)} where {@code revUpperBound == -1}.
     *
     * @param keyFrom Start key of range (inclusive). Couldn't be {@code null}.
     * @param keyTo End key of range (exclusive). {@code null} represents an unbound range.
     * @param timeoutMillis Timeout in milliseconds.
     * @return  Publisher that will provide entries corresponding to the given range.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    Publisher<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo, long timeoutMillis);

    /**
     * Retrieves entries for the given key range in lexicographic order. Short cut for
     * {@link #range(ByteArray, ByteArray, long, boolean, long)} where {@code revUpperBound == -1}.
     *
     * @param keyFrom Start key of range (inclusive). Couldn't be {@code null}.
     * @param keyTo End key of range (exclusive). {@code null} represents an unbound range.
     * @param includeTombstones Whether to include tombstone entries.
     * @param timeoutMillis Timeout in milliseconds.
     * @return Publisher that will provide entries corresponding to the given range.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    Publisher<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo, boolean includeTombstones, long timeoutMillis);

    /**
     * Retrieves entries for keys starting with the given prefix in lexicographic order.
     *
     * @param prefix Key prefix.
     * @param revUpperBound The upper bound for entry revision. {@code -1} means latest revision.
     * @param timeoutMillis Timeout in milliseconds.
     * @return Publisher that will provide entries corresponding to the given prefix and revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     */
    Publisher<Entry> prefix(ByteArray prefix, long revUpperBound, long timeoutMillis);

    /**
     * Returns a future which will hold {@link RevisionsInfo current revisions} of the metastorage leader.
     *
     * @param timeoutMillis Timeout in milliseconds.
     */
    CompletableFuture<RevisionsInfo> currentRevisions(long timeoutMillis);

    /**
     * Returns information about a revision checksum on the leader.
     *
     * @param revision Revision of interest.
     * @param timeoutMillis Timeout in milliseconds.
     */
    CompletableFuture<ChecksumInfo> checksum(long revision, long timeoutMillis);
}
