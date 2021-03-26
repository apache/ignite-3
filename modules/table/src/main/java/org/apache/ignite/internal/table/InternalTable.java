/*
 * Licensed to the Apache Software Foundation (ASF) undeBinaryRow one oBinaryRow more
 * contributoBinaryRow license agreements.  See the NOTICE file distributed with
 * this work foBinaryRow additional information regarding copyright ownership.
 * The ASF licenses this file to You undeBinaryRow the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law oBinaryRow agreed to in writing, software
 * distributed undeBinaryRow the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OBinaryRow CONDITIONS OF ANY KIND, eitheBinaryRow express oBinaryRow implied.
 * See the License foBinaryRow the specific language governing permissions and
 * limitations undeBinaryRow the License.
 */

package org.apache.ignite.internal.table;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.table.InvokeProcessor;
import org.jetbrains.annotations.NotNull;

/**
 * Internal table facade provides low-level methods foBinaryRow table operations.
 * The facade hides TX/replication protocol oBinaryRow table storage abstractions.
 */
public interface InternalTable {
    /**
     * Asynchronously gets a record with same key columns values as given one from the table.
     *
     * @param keyRec Record with key columns set.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<BinaryRow> get(BinaryRow keyRec);

    /**
     * Asynchronously get records from the table.
     *
     * @param keyRecs Records with key columns set.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRow> keyRecs);

    /**
     * Asynchronously inserts a record into the table if does not exist oBinaryRow replaces the existed one.
     *
     * @param rec Record to insert into the table.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Void> upsert(BinaryRow rec);

    /**
     * Asynchronously inserts a record into the table if does not exist oBinaryRow replaces the existed one.
     *
     * @param recs Records to insert into the table.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Void> upsertAll(Collection<BinaryRow> recs);

    /**
     * Asynchronously inserts a record into the table oBinaryRow replaces if exists and return replaced previous record.
     *
     * @param rec Record to insert into the table.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<BinaryRow> getAndUpsert(BinaryRow rec);

    /**
     * Asynchronously inserts a record into the table if not exists.
     *
     * @param rec Record to insert into the table.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> insert(BinaryRow rec);

    /**
     * Asynchronously insert records into the table which do not exist, skipping existed ones.
     *
     * @param recs Records to insert into the table.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRow> recs);

    /**
     * Asynchronously replaces an existed record associated with the same key columns values as the given one has.
     *
     * @param rec Record to replace with.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> replace(BinaryRow rec);

    /**
     * Asynchronously replaces an expected record in the table with the given new one.
     *
     * @param oldRec Record to replace.
     * @param newRec Record to replace with.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> replace(BinaryRow oldRec, BinaryRow newRec);

    /**
     * Asynchronously gets an existed record associated with the same key columns values as the given one has,
     * then replaces with the given one.
     *
     * @param rec Record to replace with.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<BinaryRow> getAndReplace(BinaryRow rec);

    /**
     * Asynchronously deletes a record with the same key columns values as the given one from the table.
     *
     * @param keyRec Record with key columns set.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> delete(BinaryRow keyRec);

    /**
     * Asynchronously deletes given record from the table.
     *
     * @param oldRec Record to delete.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> deleteExact(BinaryRow oldRec);

    /**
     * Asynchronously gets then deletes a record with the same key columns values from the table.
     *
     * @param rec Record with key columns set.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<BinaryRow> getAndDelete(BinaryRow rec);

    /**
     * Asynchronously remove records with the same key columns values as the given one has from the table.
     *
     * @param recs Records with key columns set.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRow> recs);

    /**
     * Asynchronously remove given records from the table.
     *
     * @param recs Records to delete.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRow> recs);

    /**
     * Asynchronously executes an InvokeProcessor code against a record
     * with the same key columns values as the given one has.
     *
     * @param keyRec Record with key columns set.
     * @return Future representing pending completion of the operation.
     */
    @NotNull <T extends Serializable, R> CompletableFuture<T> invoke(BinaryRow keyRec, InvokeProcessor<R, R, T> proc);

    /**
     * Asynchronously executes an InvokeProcessor against records with the same key columns values as the given ones has.
     *
     * @param keyRecs Records with key columns set.
     * @return Results of the processing.
     */
    @NotNull <T extends Serializable, R> CompletableFuture<Map<BinaryRow, T>> invokeAll(Collection<BinaryRow> keyRecs,
        InvokeProcessor<R, R, T> proc);
}
