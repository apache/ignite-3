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

package org.apache.ignite.table;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.table.criteria.CriteriaQuerySource;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Table view interface provides methods to access table records.
 *
 * @param <R> Mapped record type.
 * @see org.apache.ignite.table.mapper.Mapper
 */
public interface RecordView<R> extends DataStreamerTarget<R>, CriteriaQuerySource<R> {

    /**
     * Gets a record with the same key column values as the given one from a table.
     * Opens implicit transaction.
     *
     * @param keyRec Record with the key columns set. The record cannot be {@code null}.
     * @return Record with all columns filled from the table.
     */
    default R get(R keyRec) {
        return get(null, keyRec);
    }

    /**
     * Gets a record with the same key column values as the given one from a table.
     *
     * @param tx     Transaction or {@code null} for implicit transaction.
     * @param keyRec Record with the key columns set. The record cannot be {@code null}.
     * @return Record with all columns filled from the table.
     */
    R get(@Nullable Transaction tx, R keyRec);

    /**
     * Asynchronously gets a record with the same key column values as the given one from a table.
     * Opens implicit transaction.
     *
     * @param keyRec Record with the key columns set. The record cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     */
    default CompletableFuture<R> getAsync(R keyRec) {
        return getAsync(null, keyRec);
    }

    /**
     * Asynchronously gets a record with the same key column values as the given one from a table.
     *
     * @param tx     Transaction or {@code null} for implicit transaction.
     * @param keyRec Record with the key columns set. The record cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     */
    CompletableFuture<R> getAsync(@Nullable Transaction tx, R keyRec);

    /**
     * Gets records from a table.
     * Opens implicit transaction.
     *
     * @param keyRecs Records with key columns set. The records cannot be {@code null}.
     * @return Records with all columns filled from the table. The order of collection elements is
     *     guaranteed to be the same as the order of {@code keyRecs}. If a record does not exist, the
     *     element at the corresponding index of the resulting collection is {@code null}.
     */
    default List<R> getAll(Collection<R> keyRecs) {
        return getAll(null, keyRecs);
    }

    /**
     * Gets records from a table.
     *
     * @param tx      Transaction or {@code null} for implicit transaction.
     * @param keyRecs Records with key columns set. The records cannot be {@code null}.
     * @return Records with all columns filled from the table. The order of collection elements is
     *     guaranteed to be the same as the order of {@code keyRecs}. If a record does not exist, the
     *     element at the corresponding index of the resulting collection is {@code null}.
     */
    List<R> getAll(@Nullable Transaction tx, Collection<R> keyRecs);

    /**
     * Asynchronously gets records from a table.
     * Opens implicit transaction.
     *
     * @param keyRecs Records with the key columns set. The records cannot be {@code null}.
     * @return Future that will return records with all columns filled from the table. The order of collection elements is
     *      guaranteed to be the same as the order of {@code keyRecs}. If a record does not exist, the
     *      element at the corresponding index of the resulting collection is {@code null}.
     */
    default CompletableFuture<List<R>> getAllAsync(Collection<R> keyRecs) {
        return getAllAsync(null, keyRecs);
    }

    /**
     * Asynchronously gets records from a table.
     *
     * @param tx      Transaction or {@code null} for implicit transaction.
     * @param keyRecs Records with the key columns set. The records cannot be {@code null}.
     * @return Future that will return records with all columns filled from the table. The order of collection elements is
     *      guaranteed to be the same as the order of {@code keyRecs}. If a record does not exist, the
     *      element at the corresponding index of the resulting collection is {@code null}.
     */
    CompletableFuture<List<R>> getAllAsync(@Nullable Transaction tx, Collection<R> keyRecs);

    /**
     * Determines whether a table contains an entry for the specified key.
     * Opens implicit transaction.
     *
     * @param keyRec A record with key columns set. The key cannot be {@code null}.
     * @return {@code True} if a value exists for every specified key, {@code false} otherwise.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    default boolean contains(R keyRec) {
        return contains(null, keyRec);
    }

    /**
     * Determines whether a table contains an entry for the specified key.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param keyRec A record with key columns set. The key cannot be {@code null}.
     * @return {@code True} if a value exists for every specified key, {@code false} otherwise.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    boolean contains(@Nullable Transaction tx, R keyRec);

    /**
     * Determines whether a table contains an entry for the specified key.
     * Opens implicit transaction.
     *
     * @param keyRec A record with key columns set. The key cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    default CompletableFuture<Boolean> containsAsync(R keyRec) {
        return containsAsync(null, keyRec);
    }

    /**
     * Determines whether a table contains an entry for the specified key.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param keyRec A record with key columns set. The key cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, R keyRec);

    /**
     * Determines whether a table contains entries for all given keys.
     * Opens implicit transaction.
     *
     * @param keys Keys whose presence is to be verified. The collection and it's values cannot be {@code null}.
     * @return {@code True} if a value exists for every specified key, {@code false} otherwise.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    default boolean containsAll(Collection<R> keys) {
        return containsAll(null, keys);
    }

    /**
     * Determines whether a table contains entries for all given keys.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param keys Keys whose presence is to be verified. The collection and it's values cannot be {@code null}.
     * @return {@code True} if a value exists for every specified key, {@code false} otherwise.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    boolean containsAll(@Nullable Transaction tx, Collection<R> keys);

    /**
     * Determines whether a table contains entries for all given keys.
     * Opens implicit transaction.
     *
     * @param keys Keys whose presence is to be verified. The collection and it's values cannot be {@code null}.
     * @return Future that represents the pending completion of the operation. The result of the future will be {@code true} if a value
     *      exists for every specified key, {@code false} otherwise.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    default CompletableFuture<Boolean> containsAllAsync(Collection<R> keys) {
        return containsAllAsync(null, keys);
    }

    /**
     * Determines whether a table contains entries for all given keys.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param keys Keys whose presence is to be verified. The collection and it's values cannot be {@code null}.
     * @return Future that represents the pending completion of the operation. The result of the future will be {@code true} if a value
     *      exists for every specified key, {@code false} otherwise.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    CompletableFuture<Boolean> containsAllAsync(@Nullable Transaction tx, Collection<R> keys);

    /**
     * Inserts a record into a table, if it does not exist, or replaces an existing one.
     * Opens implicit transaction.
     *
     * @param rec Record to insert into the table. The record cannot be {@code null}.
     */
    default void upsert(R rec) {
        upsert(null, rec);
    }

    /**
     * Inserts a record into a table, if it does not exist, or replaces an existing one.
     *
     * @param tx  Transaction or {@code null} for implicit transaction.
     * @param rec Record to insert into the table. The record cannot be {@code null}.
     */
    void upsert(@Nullable Transaction tx, R rec);

    /**
     * Asynchronously inserts a record into a table, if it does not exist, or replaces the existing one.
     * Opens implicit transaction.
     *
     * @param rec Record to insert into the table. The record cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     */
    default CompletableFuture<Void> upsertAsync(R rec) {
        return upsertAsync(null, rec);
    }

    /**
     * Asynchronously inserts a record into a table, if it does not exist, or replaces the existing one.
     *
     * @param tx  Transaction or {@code null} for implicit transaction.
     * @param rec Record to insert into the table. The record cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     */
    CompletableFuture<Void> upsertAsync(@Nullable Transaction tx, R rec);

    /**
     * Inserts records into a table, if they do not exist, or replaces the existing ones.
     * Opens implicit transaction.
     *
     * @param recs Records to insert into the table. The records cannot be {@code null}.
     */
    default void upsertAll(Collection<R> recs) {
        upsertAll(null, recs);
    }

    /**
     * Inserts records into a table, if they do not exist, or replaces the existing ones.
     *
     * @param tx   Transaction or {@code null} for implicit transaction.
     * @param recs Records to insert into the table. The records cannot be {@code null}.
     */
    void upsertAll(@Nullable Transaction tx, Collection<R> recs);

    /**
     * Asynchronously inserts a record into a table, if it does not exist, or replaces the existing one.
     * Opens implicit transaction.
     *
     * @param recs Records to insert into the table. The records cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     */
    default CompletableFuture<Void> upsertAllAsync(Collection<R> recs) {
        return upsertAllAsync(null, recs);
    }

    /**
     * Asynchronously inserts a record into a table, if it does not exist, or replaces the existing one.
     *
     * @param tx   Transaction or {@code null} for implicit transaction.
     * @param recs Records to insert into the table. The records cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     */
    CompletableFuture<Void> upsertAllAsync(@Nullable Transaction tx, Collection<R> recs);

    /**
     * Inserts a record into a table, or replaces an existing record and returns the replaced record.
     * Opens implicit transaction.
     *
     * @param rec A record to insert into the table. The record cannot be {@code null}.
     * @return Replaced record or {@code null} if it did not exist.
     */
    default R getAndUpsert(R rec) {
        return getAndUpsert(null, rec);
    }

    /**
     * Inserts a record into a table, or replaces an existing record and returns the replaced record.
     *
     * @param tx  Transaction or {@code null} for implicit transaction.
     * @param rec A record to insert into the table. The record cannot be {@code null}.
     * @return Replaced record or {@code null} if it did not exist.
     */
    R getAndUpsert(@Nullable Transaction tx, R rec);

    /**
     * Asynchronously inserts a record into a table, or replaces an existing record and returns the replaced record.
     * Opens implicit transaction.
     *
     * @param rec Record to insert into the table. The record cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     */
    default CompletableFuture<R> getAndUpsertAsync(R rec) {
        return getAndUpsertAsync(null, rec);
    }

    /**
     * Asynchronously inserts a record into a table, or replaces an existing record and returns the replaced record.
     *
     * @param tx  Transaction or {@code null} for implicit transaction.
     * @param rec Record to insert into the table. The record cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     */
    CompletableFuture<R> getAndUpsertAsync(@Nullable Transaction tx, R rec);

    /**
     * Inserts a record into a table if it does not exists.
     * Opens implicit transaction.
     *
     * @param rec Record to insert into the table. The record cannot be {@code null}.
     * @return {@code True} if successful, {@code false} otherwise.
     */
    default boolean insert(R rec) {
        return insert(null, rec);
    }

    /**
     * Inserts a record into a table if it does not exists.
     *
     * @param tx  Transaction or {@code null} for implicit transaction.
     * @param rec Record to insert into the table. The record cannot be {@code null}.
     * @return {@code True} if successful, {@code false} otherwise.
     */
    boolean insert(@Nullable Transaction tx, R rec);

    /**
     * Asynchronously inserts a record into a table if it does not exists.
     * Opens implicit transaction.
     *
     * @param rec Record to insert into the table. The record cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     */
    default CompletableFuture<Boolean> insertAsync(R rec) {
        return insertAsync(null, rec);
    }

    /**
     * Asynchronously inserts a record into a table if it does not exists.
     *
     * @param tx  Transaction or {@code null} for implicit transaction.
     * @param rec Record to insert into the table. The record cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     */
    CompletableFuture<Boolean> insertAsync(@Nullable Transaction tx, R rec);

    /**
     * Inserts into a table records that do not exist, skips those that exist.
     * Opens implicit transaction.
     *
     * @param recs Records to insert into the table. The records cannot be {@code null}.
     * @return Skipped records. The order of collection elements is guaranteed to be the same as the order of {@code recs}. If a record is
     *         inserted, the element will be excluded from the collection result.
     */
    default List<R> insertAll(Collection<R> recs) {
        return insertAll(null, recs);
    }

    /**
     * Inserts into a table records that do not exist, skips those that exist.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param recs Records to insert into the table. The records cannot be {@code null}.
     * @return Skipped records. If a record is inserted, the element will be excluded from the collection result.
     */
    List<R> insertAll(@Nullable Transaction tx, Collection<R> recs);

    /**
     * Asynchronously inserts into a table records that do not exist, skips those that exist.
     * Opens implicit transaction.
     *
     * @param recs Records to insert into the table. The records cannot be {@code null}.
     * @return Future representing pending completion of the operation, with rejected rows for insertion in the result. The order of
     *         collection elements is guaranteed to be the same as the order of {@code recs}. If a record is inserted, the element will be
     *         excluded from the collection result.
     */
    default CompletableFuture<List<R>> insertAllAsync(Collection<R> recs) {
        return insertAllAsync(null, recs);
    }

    /**
     * Asynchronously inserts into a table records that do not exist, skips those that exist.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param recs Records to insert into the table. The records cannot be {@code null}.
     * @return Future representing pending completion of the operation, with rejected rows for insertion in the result. If a record is
     *         inserted, the element will be excluded from the collection result.
     */
    CompletableFuture<List<R>> insertAllAsync(@Nullable Transaction tx, Collection<R> recs);

    /**
     * Replaces an existing record associated with the same key column values as the given record.
     * Opens implicit transaction.
     *
     * @param rec Record to replace with. The record cannot be {@code null}.
     * @return {@code True} if a record was found and replaced successfully, {@code false} otherwise.
     */
    default boolean replace(R rec) {
        return replace((Transaction) null, rec);
    }

    /**
     * Replaces an existing record associated with the same key column values as the given record.
     *
     * @param tx  Transaction or {@code null} for implicit transaction.
     * @param rec Record to replace with. The record cannot be {@code null}.
     * @return {@code True} if a record was found and replaced successfully, {@code false} otherwise.
     */
    boolean replace(@Nullable Transaction tx, R rec);

    /**
     * Replaces an expected record in the table with the given new one.
     * Opens implicit transaction.
     *
     * @param oldRec Record to replace. The record cannot be {@code null}.
     * @param newRec Record to replace with. The record cannot be {@code null}.
     * @return {@code True} if a record was replaced successfully, {@code false} otherwise.
     */
    default boolean replace(R oldRec, R newRec) {
        return replace(null, oldRec, newRec);
    }

    /**
     * Replaces an expected record in the table with the given new one.
     *
     * @param tx     Transaction or {@code null} for implicit transaction.
     * @param oldRec Record to replace. The record cannot be {@code null}.
     * @param newRec Record to replace with. The record cannot be {@code null}.
     * @return {@code True} if a record was replaced successfully, {@code false} otherwise.
     */
    boolean replace(@Nullable Transaction tx, R oldRec, R newRec);

    /**
     * Asynchronously replaces an existing record associated with the same key columns values as the given record.
     * Opens implicit transaction.
     *
     * @param rec Record to replace with. The record cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     */
    default CompletableFuture<Boolean> replaceAsync(R rec) {
        return replaceAsync((Transaction) null, rec);
    }

    /**
     * Asynchronously replaces an existing record associated with the same key columns values as the given record.
     *
     * @param tx  Transaction or {@code null} for implicit transaction.
     * @param rec Record to replace with. The record cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     */
    CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, R rec);

    /**
     * Asynchronously replaces an existing record in the table with the given new one.
     * Opens implicit transaction.
     *
     * @param oldRec Record to replace. The record cannot be {@code null}.
     * @param newRec Record to replace with. The record cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     */
    default CompletableFuture<Boolean> replaceAsync(R oldRec, R newRec) {
        return replaceAsync(null, oldRec, newRec);
    }

    /**
     * Asynchronously replaces an existing record in the table with the given new one.
     *
     * @param tx     Transaction or {@code null} for implicit transaction.
     * @param oldRec Record to replace. The record cannot be {@code null}.
     * @param newRec Record to replace with. The record cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     */
    CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, R oldRec, R newRec);

    /**
     * Gets an existing record associated with the same key columns values as the given one, then replaces it with the given one.
     * Opens implicit transaction.
     *
     * @param rec Record to replace with. The record cannot be {@code null}.
     * @return Replaced record or {@code null} if it did not exist.
     */
    default R getAndReplace(R rec) {
        return getAndReplace(null, rec);
    }

    /**
     * Gets an existing record associated with the same key columns values as the given one, then replaces it with the given one.
     *
     * @param tx  Transaction or {@code null} for implicit transaction.
     * @param rec Record to replace with. The record cannot be {@code null}.
     * @return Replaced record or {@code null} if it did not exist.
     */
    R getAndReplace(@Nullable Transaction tx, R rec);

    /**
     * Asynchronously gets an existing record associated with the same key column values as the given one,
     * then replaces it with the given one.
     * Opens implicit transaction.
     *
     * @param rec Record to replace with. The record cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     */
    default CompletableFuture<R> getAndReplaceAsync(R rec) {
        return getAndReplaceAsync(null, rec);
    }

    /**
     * Asynchronously gets an existing record associated with the same key column values as the given one,
     * then replaces it with the given one.
     *
     * @param tx  Transaction or {@code null} for implicit transaction.
     * @param rec Record to replace with. The record cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     */
    CompletableFuture<R> getAndReplaceAsync(@Nullable Transaction tx, R rec);

    /**
     * Deletes a record with the same key column values as the given one from a table.
     * Opens implicit transaction.
     *
     * @param keyRec Record with the key columns set. The record cannot be {@code null}.
     * @return {@code True} if removed successfully, {@code false} otherwise.
     */
    default boolean delete(R keyRec) {
        return delete(null, keyRec);
    }

    /**
     * Deletes a record with the same key column values as the given one from a table.
     *
     * @param tx     Transaction or {@code null} for implicit transaction.
     * @param keyRec Record with the key columns set. The record cannot be {@code null}.
     * @return {@code True} if removed successfully, {@code false} otherwise.
     */
    boolean delete(@Nullable Transaction tx, R keyRec);

    /**
     * Asynchronously deletes a record with the same key column values as the given one from a table.
     * Opens implicit transaction.
     *
     * @param keyRec Record with the key columns set. The record cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     */
    default CompletableFuture<Boolean> deleteAsync(R keyRec) {
        return deleteAsync(null, keyRec);
    }

    /**
     * Asynchronously deletes a record with the same key column values as the given one from a table.
     *
     * @param tx     Transaction or {@code null} for implicit transaction.
     * @param keyRec Record with the key columns set. The record cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     */
    CompletableFuture<Boolean> deleteAsync(@Nullable Transaction tx, R keyRec);

    /**
     * Deletes the given record from a table.
     * Opens implicit transaction.
     *
     * @param rec Record to delete. The record cannot be {@code null}.
     * @return {@code True} if removed successfully, {@code false} otherwise.
     */
    default boolean deleteExact(R rec) {
        return deleteExact(null, rec);
    }

    /**
     * Deletes the given record from a table.
     *
     * @param tx  Transaction or {@code null} for implicit transaction.
     * @param rec Record to delete. The record cannot be {@code null}.
     * @return {@code True} if removed successfully, {@code false} otherwise.
     */
    boolean deleteExact(@Nullable Transaction tx, R rec);

    /**
     * Asynchronously deletes the given record from a table.
     * Opens implicit transaction.
     *
     * @param rec Record to delete. The record cannot be {@code null}.
     * @return Future tha represents the pending completion of the operation.
     */
    default CompletableFuture<Boolean> deleteExactAsync(R rec) {
        return deleteExactAsync(null, rec);
    }

    /**
     * Asynchronously deletes the given record from a table.
     *
     * @param tx  Transaction or {@code null} for implicit transaction.
     * @param rec Record to delete. The record cannot be {@code null}.
     * @return Future tha represents the pending completion of the operation.
     */
    CompletableFuture<Boolean> deleteExactAsync(@Nullable Transaction tx, R rec);

    /**
     * Gets and deletes from a table a record with the same key column values as the given one.
     * Opens implicit transaction.
     *
     * @param keyRec Record with the key columns set. The record cannot be {@code null}.
     * @return Removed record or {@code null} if it did not exist.
     */
    default R getAndDelete(R keyRec) {
        return getAndDelete(null, keyRec);
    }

    /**
     * Gets and deletes from a table a record with the same key column values as the given one.
     *
     * @param tx     Transaction or {@code null} for implicit transaction.
     * @param keyRec Record with the key columns set. The record cannot be {@code null}.
     * @return Removed record or {@code null} if it did not exist.
     */
    R getAndDelete(@Nullable Transaction tx, R keyRec);

    /**
     * Asynchronously gets and deletes from a table a record with the same key columns values as the given one.
     * Opens implicit transaction.
     *
     * @param keyRec Record with the key columns set. The record cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     */
    default CompletableFuture<R> getAndDeleteAsync(R keyRec) {
        return getAndDeleteAsync(null, keyRec);
    }

    /**
     * Asynchronously gets and deletes from a table a record with the same key columns values as the given one.
     *
     * @param tx     Transaction or {@code null} for implicit transaction.
     * @param keyRec Record with the key columns set. The record cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     */
    CompletableFuture<R> getAndDeleteAsync(@Nullable Transaction tx, R keyRec);

    /**
     * Removes from a table records with the same key column values as the given one.
     * Opens implicit transaction.
     *
     * @param keyRecs Records with the key columns set. The records cannot be {@code null}.
     * @return Records with the key columns set that did not exist. The order of collection elements is guaranteed to be the same as the
     *         order of {@code keyRecs}. If a record is removed, the element will be excluded from the collection result.
     */
    default List<R> deleteAll(Collection<R> keyRecs) {
        return deleteAll(null, keyRecs);
    }

    /**
     * Removes from a table records with the same key column values as the given one.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param keyRecs Records with the key columns set. The records cannot be {@code null}.
     * @return Records with the key columns set that did not exist.
     *         If a record is removed, the element will be excluded from the collection result.
     */
    List<R> deleteAll(@Nullable Transaction tx, Collection<R> keyRecs);

    /**
     * Removes all entries from a table records.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     */
    void deleteAll(@Nullable Transaction tx);

    /**
     * Asynchronously removes from a table records with the same key column values as the given one.
     * Opens implicit transaction.
     *
     * @param keyRecs Records with the key columns set. The records cannot be {@code null}.
     * @return Future represents the pending completion of the operation, with rejected rows for deletion in the result. The order of
     *         collection elements is guaranteed to be the same as the order of {@code keyRecs}. If a record is removed, the element will be
     *         excluded from the collection result.
     */
    default CompletableFuture<List<R>> deleteAllAsync(Collection<R> keyRecs) {
        return deleteAllAsync(null, keyRecs);
    }

    /**
     * Asynchronously removes from a table records with the same key column values as the given one.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param keyRecs Records with the key columns set. The records cannot be {@code null}.
     * @return Future represents the pending completion of the operation, with rejected rows for deletion in the result.
     *         If a record is removed, the element will be excluded from the collection result.
     */
    CompletableFuture<List<R>> deleteAllAsync(@Nullable Transaction tx, Collection<R> keyRecs);

    /**
     * Asynchronously removes all entries from a table records.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @return Future represents the pending completion of the operation.
     */
    CompletableFuture<Void> deleteAllAsync(@Nullable Transaction tx);

    /**
     * Remove the given records from a table.
     * Opens implicit transaction.
     *
     * @param recs Records to delete. The records cannot be {@code null}.
     * @return Records that were not deleted. The order of collection elements is guaranteed to be the same as the order of {@code recs}. If
     *         a record is removed, the element will be excluded from the collection result.
     */
    default List<R> deleteAllExact(Collection<R> recs) {
        return deleteAllExact(null, recs);
    }

    /**
     * Remove the given records from a table.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param recs Records to delete. The records cannot be {@code null}.
     * @return Records that were not deleted. If a record is removed, the element will be excluded from the collection result.
     */
    List<R> deleteAllExact(@Nullable Transaction tx, Collection<R> recs);

    /**
     * Asynchronously removes the given records from a table.
     * Opens implicit transaction.
     *
     * @param recs Records to delete. The records cannot be {@code null}.
     * @return Future represents the pending completion of the operation, with rejected rows for deletion in the result. The order of
     *         collection elements is guaranteed to be the same as the order of {@code recs}. If a record is removed, the element will be
     *         excluded from the collection result.
     */
    default CompletableFuture<List<R>> deleteAllExactAsync(Collection<R> recs) {
        return deleteAllExactAsync(null, recs);
    }

    /**
     * Asynchronously removes the given records from a table.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param recs Records to delete. The records cannot be {@code null}.
     * @return Future represents the pending completion of the operation, with rejected rows for deletion in the result.
     *         If a record is removed, the element will be excluded from the collection result.
     */
    CompletableFuture<List<R>> deleteAllExactAsync(@Nullable Transaction tx, Collection<R> recs);
}
