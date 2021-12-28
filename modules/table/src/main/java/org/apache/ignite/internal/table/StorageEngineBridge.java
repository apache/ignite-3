/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.table;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Internal table facade provides low-level methods for table operations. The facade hides TX/replication protocol over table storage
 * abstractions.
 */
public interface StorageEngineBridge {
    /**
     * Asynchronously gets a row with same key columns values as given one from the table.
     *
     * @param keyRow Row with key columns set.
     * @param tx     The transaction.
     * @return Future representing pending completion of the operation.
     * @throws LockException If a lock can't be acquired by some reason.
     */
    CompletableFuture<BinaryRow> get(IgniteUuid tableId, BinaryRow keyRow, @Nullable InternalTransaction tx);

    /**
     * Asynchronously get rows from the table.
     *
     * @param keyRows Rows with key columns set.
     * @param tx      The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Collection<BinaryRow>> getAll(IgniteUuid tableId, Collection<BinaryRow> keyRows, @Nullable InternalTransaction tx);

    /**
     * Asynchronously inserts a row into the table if does not exist or replaces the existed one.
     *
     * @param row Row to insert into the table.
     * @param tx  The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Void> upsert(IgniteUuid tableId, BinaryRow row, @Nullable InternalTransaction tx);

    /**
     * Asynchronously inserts a row into the table if does not exist or replaces the existed one.
     *
     * @param rows Rows to insert into the table.
     * @param tx   The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Void> upsertAll(IgniteUuid tableId, Collection<BinaryRow> rows, @Nullable InternalTransaction tx);

    /**
     * Asynchronously inserts a row into the table or replaces if exists and return replaced previous row.
     *
     * @param row Row to insert into the table.
     * @param tx  The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<BinaryRow> getAndUpsert(IgniteUuid tableId, BinaryRow row, @Nullable InternalTransaction tx);

    /**
     * Asynchronously inserts a row into the table if not exists.
     *
     * @param row Row to insert into the table.
     * @param tx  The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> insert(IgniteUuid tableId, BinaryRow row, @Nullable InternalTransaction tx);

    /**
     * Asynchronously insert rows into the table which do not exist, skipping existed ones.
     *
     * @param rows Rows to insert into the table.
     * @param tx   The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Collection<BinaryRow>> insertAll(IgniteUuid tableId, Collection<BinaryRow> rows, @Nullable InternalTransaction tx);

    /**
     * Asynchronously replaces an existed row associated with the same key columns values as the given one has.
     *
     * @param row Row to replace with.
     * @param tx  The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> replace(IgniteUuid tableId, BinaryRow row, @Nullable InternalTransaction tx);

    /**
     * Asynchronously replaces an expected row in the table with the given new one.
     *
     * @param oldRow Row to replace.
     * @param newRow Row to replace with.
     * @param tx     The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> replace(IgniteUuid tableId, BinaryRow oldRow, BinaryRow newRow, @Nullable InternalTransaction tx);

    /**
     * Asynchronously gets an existed row associated with the same key columns values as the given one has, then replaces with the given
     * one.
     *
     * @param row Row to replace with.
     * @param tx  The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<BinaryRow> getAndReplace(IgniteUuid tableId, BinaryRow row, @Nullable InternalTransaction tx);

    /**
     * Asynchronously deletes a row with the same key columns values as the given one from the table.
     *
     * @param keyRow Row with key columns set.
     * @param tx     The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> delete(IgniteUuid tableId, BinaryRow keyRow, @Nullable InternalTransaction tx);

    /**
     * Asynchronously deletes given row from the table.
     *
     * @param oldRow Row to delete.
     * @param tx     The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> deleteExact(IgniteUuid tableId, BinaryRow oldRow, @Nullable InternalTransaction tx);

    /**
     * Asynchronously gets then deletes a row with the same key columns values from the table.
     *
     * @param row Row with key columns set.
     * @param tx  The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<BinaryRow> getAndDelete(IgniteUuid tableId, BinaryRow row, @Nullable InternalTransaction tx);

    /**
     * Asynchronously remove rows with the same key columns values as the given one has from the table.
     *
     * @param rows Rows with key columns set.
     * @param tx   The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Collection<BinaryRow>> deleteAll(IgniteUuid tableId, Collection<BinaryRow> rows, @Nullable InternalTransaction tx);

    /**
     * Asynchronously remove given rows from the table.
     *
     * @param rows Rows to delete.
     * @param tx   The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Collection<BinaryRow>> deleteAllExact(
            IgniteUuid tableId, Collection<BinaryRow> rows, @Nullable InternalTransaction tx);

    /**
     * Scans given partition, providing {@link Publisher} that reactively notifies about partition rows.
     *
     * @param p  The partition.
     * @param tx The transaction.
     * @return {@link Publisher} that reactively notifies about partition rows.
     */
    @NotNull Publisher<BinaryRow> scan(IgniteUuid tableId, int p, @Nullable InternalTransaction tx);
}
