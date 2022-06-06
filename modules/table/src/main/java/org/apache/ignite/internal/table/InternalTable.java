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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Internal table facade provides low-level methods for table operations. The facade hides TX/replication protocol over table storage
 * abstractions.
 */
public interface InternalTable extends AutoCloseable {
    /**
     * Gets a storage for the table.
     *
     * @return Table storage.
     */
    @NotNull TableStorage storage();

    /**
     * Gets a table id.
     *
     * @return Table id as UUID.
     */
    @NotNull UUID tableId();

    /**
     * Gets a name of the table.
     *
     * @return Table name.
     */
    @NotNull String name();

    /**
     * Asynchronously gets a row with same key columns values as given one from the table.
     *
     * @param keyRow Row with key columns set.
     * @param tx     The transaction.
     * @return Future representing pending completion of the operation.
     * @throws LockException If a lock can't be acquired by some reason.
     */
    CompletableFuture<BinaryRow> get(BinaryRowEx keyRow, @Nullable InternalTransaction tx);

    /**
     * Asynchronously get rows from the table.
     *
     * @param keyRows Rows with key columns set.
     * @param tx      The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRowEx> keyRows, @Nullable InternalTransaction tx);

    /**
     * Asynchronously inserts a row into the table if does not exist or replaces the existed one.
     *
     * @param row Row to insert into the table.
     * @param tx  The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Void> upsert(BinaryRowEx row, @Nullable InternalTransaction tx);

    /**
     * Asynchronously inserts a row into the table if does not exist or replaces the existed one.
     *
     * @param rows Rows to insert into the table.
     * @param tx   The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Void> upsertAll(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx);

    /**
     * Asynchronously inserts a row into the table or replaces if exists and return replaced previous row.
     *
     * @param row Row to insert into the table.
     * @param tx  The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<BinaryRow> getAndUpsert(BinaryRowEx row, @Nullable InternalTransaction tx);

    /**
     * Asynchronously inserts a row into the table if not exists.
     *
     * @param row Row to insert into the table.
     * @param tx  The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> insert(BinaryRowEx row, @Nullable InternalTransaction tx);

    /**
     * Asynchronously insert rows into the table which do not exist, skipping existed ones.
     *
     * @param rows Rows to insert into the table.
     * @param tx   The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx);

    /**
     * Asynchronously replaces an existed row associated with the same key columns values as the given one has.
     *
     * @param row Row to replace with.
     * @param tx  The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> replace(BinaryRowEx row, @Nullable InternalTransaction tx);

    /**
     * Asynchronously replaces an expected row in the table with the given new one.
     *
     * @param oldRow Row to replace.
     * @param newRow Row to replace with.
     * @param tx     The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> replace(BinaryRowEx oldRow, BinaryRowEx newRow, @Nullable InternalTransaction tx);

    /**
     * Asynchronously gets an existed row associated with the same key columns values as the given one has, then replaces with the given
     * one.
     *
     * @param row Row to replace with.
     * @param tx  The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<BinaryRow> getAndReplace(BinaryRowEx row, @Nullable InternalTransaction tx);

    /**
     * Asynchronously deletes a row with the same key columns values as the given one from the table.
     *
     * @param keyRow Row with key columns set.
     * @param tx     The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> delete(BinaryRowEx keyRow, @Nullable InternalTransaction tx);

    /**
     * Asynchronously deletes given row from the table.
     *
     * @param oldRow Row to delete.
     * @param tx     The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> deleteExact(BinaryRowEx oldRow, @Nullable InternalTransaction tx);

    /**
     * Asynchronously gets then deletes a row with the same key columns values from the table.
     *
     * @param row Row with key columns set.
     * @param tx  The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<BinaryRow> getAndDelete(BinaryRowEx row, @Nullable InternalTransaction tx);

    /**
     * Asynchronously remove rows with the same key columns values as the given one has from the table.
     *
     * @param rows Rows with key columns set.
     * @param tx   The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx);

    /**
     * Asynchronously remove given rows from the table.
     *
     * @param rows Rows to delete.
     * @param tx   The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx);

    /**
     * Returns a partition for a key.
     *
     * @param keyRow The key.
     * @return The partition.
     */
    int partition(BinaryRowEx keyRow);

    /**
     * Scans given partition, providing {@link Publisher} that reactively notifies about partition rows.
     *
     * @param p  The partition.
     * @param tx The transaction.
     * @return {@link Publisher} that reactively notifies about partition rows.
     */
    @NotNull Publisher<BinaryRow> scan(int p, @Nullable InternalTransaction tx);

    /**
     * Gets a count of partitions of the table.
     *
     * @return Count of partitions.
     */
    int partitions();

    /**
     * Gets a list of current table assignments.
     *
     * <p>Returns a list where on the i-th place resides a node id that considered as a leader for
     * the i-th partition on the moment of invocation.
     *
     * @return List of current assignments.
     */
    @NotNull List<String> assignments();

    /**
     * Returns cluster node that is the leader of the corresponding partition group or throws an exception if
     * it cannot be found.
     *
     * @param partition partition number
     * @return leader node of the partition group corresponding to the partition
     */
    ClusterNode leaderAssignment(int partition);

    //TODO: IGNITE-14488. Add invoke() methods.
}
