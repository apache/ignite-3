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

package org.apache.ignite.internal.client.table;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Client record view implementation for binary user-object representation.
 */
public class ClientRecordBinaryView implements RecordView<Tuple> {
    /** Underlying table. */
    private final ClientTable tbl;

    /**
     * Constructor.
     *
     * @param tbl Table.
     */
    public ClientRecordBinaryView(ClientTable tbl) {
        assert tbl != null;

        this.tbl = tbl;
    }

    /** {@inheritDoc} */
    @Override
    public Tuple get(@Nullable Transaction tx, @NotNull Tuple keyRec) {
        return getAsync(tx, keyRec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAsync(@Nullable Transaction tx, @NotNull Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                tx,
                (s, w) -> tbl.writeTuple(tx, keyRec, s, w, true),
                (s, r) -> ClientTable.readValueTuple(s, r, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Tuple> getAll(@Nullable Transaction tx, @NotNull Collection<Tuple> keyRecs) {
        return getAllAsync(tx, keyRecs).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> getAllAsync(@Nullable Transaction tx, @NotNull Collection<Tuple> keyRecs) {
        Objects.requireNonNull(keyRecs);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_ALL,
                tx,
                (s, w) -> tbl.writeTuples(tx, keyRecs, s, w, true),
                tbl::readTuplesNullable,
                Collections.emptyList());
    }

    /** {@inheritDoc} */
    @Override
    public void upsert(@Nullable Transaction tx, @NotNull Tuple rec) {
        upsertAsync(tx, rec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> upsertAsync(@Nullable Transaction tx, @NotNull Tuple rec) {
        Objects.requireNonNull(rec);
        // TODO IGNITE-15194: Convert Tuple to a schema-order Array as a first step.
        // If it does not match the latest schema, then request latest and convert again.
        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT,
                tx,
                (s, w) -> tbl.writeTuple(tx, rec, s, w),
                r -> null);
    }

    /** {@inheritDoc} */
    @Override
    public void upsertAll(@Nullable Transaction tx, @NotNull Collection<Tuple> recs) {
        upsertAllAsync(tx, recs).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> upsertAllAsync(@Nullable Transaction tx, @NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT_ALL,
                tx,
                (s, w) -> tbl.writeTuples(tx, recs, s, w, false),
                r -> null);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndUpsert(@Nullable Transaction tx, @NotNull Tuple rec) {
        return getAndUpsertAsync(tx, rec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAndUpsertAsync(@Nullable Transaction tx, @NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_UPSERT,
                tx,
                (s, w) -> tbl.writeTuple(tx, rec, s, w, false),
                (s, r) -> ClientTable.readValueTuple(s, r, rec));
    }

    /** {@inheritDoc} */
    @Override
    public boolean insert(@Nullable Transaction tx, @NotNull Tuple rec) {
        return insertAsync(tx, rec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> insertAsync(@Nullable Transaction tx, @NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_INSERT,
                tx,
                (s, w) -> tbl.writeTuple(tx, rec, s, w, false),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Tuple> insertAll(@Nullable Transaction tx, @NotNull Collection<Tuple> recs) {
        return insertAllAsync(tx, recs).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> insertAllAsync(@Nullable Transaction tx, @NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_INSERT_ALL,
                tx,
                (s, w) -> tbl.writeTuples(tx, recs, s, w, false),
                tbl::readTuples,
                Collections.emptyList());
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, @NotNull Tuple rec) {
        return replaceAsync(tx, rec).join();
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, @NotNull Tuple oldRec, @NotNull Tuple newRec) {
        return replaceAsync(tx, oldRec, newRec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, @NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE,
                tx,
                (s, w) -> tbl.writeTuple(tx, rec, s, w, false),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, @NotNull Tuple oldRec, @NotNull Tuple newRec) {
        Objects.requireNonNull(oldRec);
        Objects.requireNonNull(newRec);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE_EXACT,
                tx,
                (s, w) -> {
                    tbl.writeTuple(tx, oldRec, s, w, false, false);
                    tbl.writeTuple(tx, newRec, s, w, false, true);
                },
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndReplace(@Nullable Transaction tx, @NotNull Tuple rec) {
        return getAndReplaceAsync(tx, rec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAndReplaceAsync(@Nullable Transaction tx, @NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_REPLACE,
                tx,
                (s, w) -> tbl.writeTuple(tx, rec, s, w, false),
                (s, r) -> ClientTable.readValueTuple(s, r, rec));
    }

    /** {@inheritDoc} */
    @Override
    public boolean delete(@Nullable Transaction tx, @NotNull Tuple keyRec) {
        return deleteAsync(tx, keyRec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> deleteAsync(@Nullable Transaction tx, @NotNull Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE,
                tx,
                (s, w) -> tbl.writeTuple(tx, keyRec, s, w, true),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public boolean deleteExact(@Nullable Transaction tx, @NotNull Tuple rec) {
        return deleteExactAsync(tx, rec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> deleteExactAsync(@Nullable Transaction tx, @NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE_EXACT,
                tx,
                (s, w) -> tbl.writeTuple(tx, rec, s, w, false),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndDelete(@Nullable Transaction tx, @NotNull Tuple keyRec) {
        return getAndDeleteAsync(tx, keyRec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAndDeleteAsync(@Nullable Transaction tx, @NotNull Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_DELETE,
                tx,
                (s, w) -> tbl.writeTuple(tx, keyRec, s, w, true),
                (s, r) -> ClientTable.readValueTuple(s, r, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Tuple> deleteAll(@Nullable Transaction tx, @NotNull Collection<Tuple> keyRecs) {
        return deleteAllAsync(tx, keyRecs).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> deleteAllAsync(@Nullable Transaction tx, @NotNull Collection<Tuple> keyRecs) {
        Objects.requireNonNull(keyRecs);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_DELETE_ALL,
                tx,
                (s, w) -> tbl.writeTuples(tx, keyRecs, s, w, true),
                (s, r) -> tbl.readTuples(s, r, true),
                Collections.emptyList());
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Tuple> deleteAllExact(@Nullable Transaction tx, @NotNull Collection<Tuple> recs) {
        return deleteAllExactAsync(tx, recs).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> deleteAllExactAsync(@Nullable Transaction tx, @NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_DELETE_ALL_EXACT,
                tx,
                (s, w) -> tbl.writeTuples(tx, recs, s, w, false),
                tbl::readTuples,
                Collections.emptyList());
    }

    /** {@inheritDoc} */
    @Override
    public <T extends Serializable> T invoke(@Nullable Transaction tx, @NotNull Tuple keyRec, InvokeProcessor<Tuple, Tuple, T> proc) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <T extends Serializable> CompletableFuture<T> invokeAsync(
            @Nullable Transaction tx,
            @NotNull Tuple keyRec,
            InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public <T extends Serializable> Map<Tuple, T> invokeAll(
            @Nullable Transaction tx,
            @NotNull Collection<Tuple> keyRecs,
            InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <T extends Serializable> CompletableFuture<Map<Tuple, T>> invokeAllAsync(
            @Nullable Transaction tx,
            @NotNull Collection<Tuple> keyRecs,
            InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }
}
