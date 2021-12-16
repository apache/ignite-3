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
    public Tuple get(@NotNull Tuple keyRec, @Nullable Transaction tx) {
        return getAsync(keyRec, tx).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAsync(@NotNull Tuple keyRec, @Nullable Transaction tx) {
        Objects.requireNonNull(keyRec);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (schema, out) -> tbl.writeTuple(keyRec, schema, out, true),
                (inSchema, in) -> ClientTable.readValueTuple(inSchema, in, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Tuple> getAll(@NotNull Collection<Tuple> keyRecs, @Nullable Transaction tx) {
        return getAllAsync(keyRecs, tx).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> getAllAsync(@NotNull Collection<Tuple> keyRecs, @Nullable Transaction tx) {
        Objects.requireNonNull(keyRecs);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_ALL,
                (s, w) -> tbl.writeTuples(keyRecs, s, w, true),
                tbl::readTuplesNullable,
                Collections.emptyList());
    }

    /** {@inheritDoc} */
    @Override
    public void upsert(@NotNull Tuple rec, @Nullable Transaction tx) {
        upsertAsync(rec, tx).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> upsertAsync(@NotNull Tuple rec, @Nullable Transaction tx) {
        Objects.requireNonNull(rec);
        // TODO: Transactions IGNITE-15240
        // TODO IGNITE-15194: Convert Tuple to a schema-order Array as a first step.
        // If it does not match the latest schema, then request latest and convert again.
        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT,
                (s, w) -> tbl.writeTuple(rec, s, w),
                r -> null);
    }

    /** {@inheritDoc} */
    @Override
    public void upsertAll(@NotNull Collection<Tuple> recs, @Nullable Transaction tx) {
        upsertAllAsync(recs, tx).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> upsertAllAsync(@NotNull Collection<Tuple> recs, @Nullable Transaction tx) {
        Objects.requireNonNull(recs);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT_ALL,
                (s, w) -> tbl.writeTuples(recs, s, w, false),
                r -> null);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndUpsert(@NotNull Tuple rec, @Nullable Transaction tx) {
        return getAndUpsertAsync(rec, tx).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAndUpsertAsync(@NotNull Tuple rec, @Nullable Transaction tx) {
        Objects.requireNonNull(rec);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_UPSERT,
                (s, w) -> tbl.writeTuple(rec, s, w, false),
                (schema, in) -> ClientTable.readValueTuple(schema, in, rec));
    }

    /** {@inheritDoc} */
    @Override
    public boolean insert(@NotNull Tuple rec, @Nullable Transaction tx) {
        return insertAsync(rec, tx).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> insertAsync(@NotNull Tuple rec, @Nullable Transaction tx) {
        Objects.requireNonNull(rec);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_INSERT,
                (s, w) -> tbl.writeTuple(rec, s, w, false),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Tuple> insertAll(@NotNull Collection<Tuple> recs, @Nullable Transaction tx) {
        return insertAllAsync(recs, tx).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> insertAllAsync(@NotNull Collection<Tuple> recs, @Nullable Transaction tx) {
        Objects.requireNonNull(recs);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_INSERT_ALL,
                (s, w) -> tbl.writeTuples(recs, s, w, false),
                tbl::readTuples,
                Collections.emptyList());
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@NotNull Tuple rec, @Nullable Transaction tx) {
        return replaceAsync(rec, tx).join();
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@NotNull Tuple oldRec, @NotNull Tuple newRec, @Nullable Transaction tx) {
        return replaceAsync(oldRec, newRec, tx).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple rec, @Nullable Transaction tx) {
        Objects.requireNonNull(rec);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE,
                (s, w) -> tbl.writeTuple(rec, s, w, false),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple oldRec, @NotNull Tuple newRec, @Nullable Transaction tx) {
        Objects.requireNonNull(oldRec);
        Objects.requireNonNull(newRec);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE_EXACT,
                (s, w) -> {
                    tbl.writeTuple(oldRec, s, w, false, false);
                    tbl.writeTuple(newRec, s, w, false, true);
                },
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndReplace(@NotNull Tuple rec, @Nullable Transaction tx) {
        return getAndReplaceAsync(rec, tx).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAndReplaceAsync(@NotNull Tuple rec, @Nullable Transaction tx) {
        Objects.requireNonNull(rec);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_REPLACE,
                (s, w) -> tbl.writeTuple(rec, s, w, false),
                (schema, in) -> ClientTable.readValueTuple(schema, in, rec));
    }

    /** {@inheritDoc} */
    @Override
    public boolean delete(@NotNull Tuple keyRec, @Nullable Transaction tx) {
        return deleteAsync(keyRec, tx).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> deleteAsync(@NotNull Tuple keyRec, @Nullable Transaction tx) {
        Objects.requireNonNull(keyRec);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE,
                (s, w) -> tbl.writeTuple(keyRec, s, w, true),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public boolean deleteExact(@NotNull Tuple rec, @Nullable Transaction tx) {
        return deleteExactAsync(rec, tx).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> deleteExactAsync(@NotNull Tuple rec, @Nullable Transaction tx) {
        Objects.requireNonNull(rec);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE_EXACT,
                (s, w) -> tbl.writeTuple(rec, s, w, false),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndDelete(@NotNull Tuple keyRec, @Nullable Transaction tx) {
        return getAndDeleteAsync(keyRec, tx).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAndDeleteAsync(@NotNull Tuple keyRec, @Nullable Transaction tx) {
        Objects.requireNonNull(keyRec);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_DELETE,
                (s, w) -> tbl.writeTuple(keyRec, s, w, true),
                (schema, in) -> ClientTable.readValueTuple(schema, in, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Tuple> deleteAll(@NotNull Collection<Tuple> keyRecs, @Nullable Transaction tx) {
        return deleteAllAsync(keyRecs, tx).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> deleteAllAsync(@NotNull Collection<Tuple> keyRecs, @Nullable Transaction tx) {
        Objects.requireNonNull(keyRecs);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_DELETE_ALL,
                (s, w) -> tbl.writeTuples(keyRecs, s, w, true),
                (schema, in) -> tbl.readTuples(schema, in, true),
                Collections.emptyList());
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Tuple> deleteAllExact(@NotNull Collection<Tuple> recs, @Nullable Transaction tx) {
        return deleteAllExactAsync(recs, tx).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> deleteAllExactAsync(@NotNull Collection<Tuple> recs, @Nullable Transaction tx) {
        Objects.requireNonNull(recs);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_DELETE_ALL_EXACT,
                (s, w) -> tbl.writeTuples(recs, s, w, false),
                tbl::readTuples,
                Collections.emptyList());
    }

    /** {@inheritDoc} */
    @Override
    public <T extends Serializable> T invoke(@NotNull Tuple keyRec, InvokeProcessor<Tuple, Tuple, T> proc, @Nullable Transaction tx) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <T extends Serializable> CompletableFuture<T> invokeAsync(
            @NotNull Tuple keyRec,
            InvokeProcessor<Tuple, Tuple, T> proc,
            @Nullable Transaction tx) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public <T extends Serializable> Map<Tuple, T> invokeAll(
            @NotNull Collection<Tuple> keyRecs,
            InvokeProcessor<Tuple, Tuple, T> proc,
            @Nullable Transaction tx) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <T extends Serializable> CompletableFuture<Map<Tuple, T>> invokeAllAsync(
            @NotNull Collection<Tuple> keyRecs,
            InvokeProcessor<Tuple, Tuple, T> proc,
            @Nullable Transaction tx) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }
}
