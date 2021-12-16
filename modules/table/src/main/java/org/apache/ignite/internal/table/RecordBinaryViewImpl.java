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

import static java.util.stream.Collectors.toList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Table view implementation for binary objects.
 */
public class RecordBinaryViewImpl extends AbstractTableView implements RecordView<Tuple> {
    /** Marshaller. */
    private final TupleMarshallerImpl marsh;

    /**
     * Constructor.
     *
     * @param tbl       The table.
     * @param schemaReg Table schema registry.
     */
    public RecordBinaryViewImpl(InternalTable tbl, SchemaRegistry schemaReg) {
        super(tbl, schemaReg);

        marsh = new TupleMarshallerImpl(schemaReg);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple get(@NotNull Tuple keyRec, @Nullable Transaction tx) {
        return sync(getAsync(keyRec, tx));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAsync(@NotNull Tuple keyRec, @Nullable Transaction tx) {
        Objects.requireNonNull(keyRec);

        final Row keyRow = marshal(keyRec, true); // Convert to portable format to pass TX/storage layer.

        return tbl.get(keyRow, (InternalTransaction) tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Tuple> getAll(@NotNull Collection<Tuple> keyRecs, @Nullable Transaction tx) {
        return sync(getAllAsync(keyRecs, tx));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> getAllAsync(@NotNull Collection<Tuple> keyRecs, @Nullable Transaction tx) {
        Objects.requireNonNull(keyRecs);

        return tbl.getAll(mapToBinary(keyRecs, true), (InternalTransaction) tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override
    public void upsert(@NotNull Tuple rec, @Nullable Transaction tx) {
        sync(upsertAsync(rec, tx));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> upsertAsync(@NotNull Tuple rec, @Nullable Transaction tx) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.upsert(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public void upsertAll(@NotNull Collection<Tuple> recs, @Nullable Transaction tx) {
        sync(upsertAllAsync(recs, tx));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> upsertAllAsync(@NotNull Collection<Tuple> recs, @Nullable Transaction tx) {
        Objects.requireNonNull(recs);

        return tbl.upsertAll(mapToBinary(recs, false), (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndUpsert(@NotNull Tuple rec, @Nullable Transaction tx) {
        return sync(getAndUpsertAsync(rec, tx));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAndUpsertAsync(@NotNull Tuple rec, @Nullable Transaction tx) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.getAndUpsert(row, (InternalTransaction) tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override
    public boolean insert(@NotNull Tuple rec, @Nullable Transaction tx) {
        return sync(insertAsync(rec, tx));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> insertAsync(@NotNull Tuple rec, @Nullable Transaction tx) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.insert(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Tuple> insertAll(@NotNull Collection<Tuple> recs, @Nullable Transaction tx) {
        return sync(insertAllAsync(recs, tx));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> insertAllAsync(@NotNull Collection<Tuple> recs, @Nullable Transaction tx) {
        Objects.requireNonNull(recs);

        return tbl.insertAll(mapToBinary(recs, false), (InternalTransaction) tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@NotNull Tuple rec, @Nullable Transaction tx) {
        return sync(replaceAsync(rec, tx));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@NotNull Tuple oldRec, @NotNull Tuple newRec, @Nullable Transaction tx) {
        return sync(replaceAsync(oldRec, newRec, tx));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple rec, @Nullable Transaction tx) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.replace(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple oldRec, @NotNull Tuple newRec, @Nullable Transaction tx) {
        Objects.requireNonNull(oldRec);
        Objects.requireNonNull(newRec);

        final Row oldRow = marshal(oldRec, false);
        final Row newRow = marshal(newRec, false);

        return tbl.replace(oldRow, newRow, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndReplace(@NotNull Tuple rec, @Nullable Transaction tx) {
        return sync(getAndReplaceAsync(rec, tx));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAndReplaceAsync(@NotNull Tuple rec, @Nullable Transaction tx) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.getAndReplace(row, (InternalTransaction) tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override
    public boolean delete(@NotNull Tuple keyRec, @Nullable Transaction tx) {
        return sync(deleteAsync(keyRec, tx));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> deleteAsync(@NotNull Tuple keyRec, @Nullable Transaction tx) {
        Objects.requireNonNull(keyRec);

        final Row keyRow = marshal(keyRec, true);

        return tbl.delete(keyRow, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean deleteExact(@NotNull Tuple rec, @Nullable Transaction tx) {
        return sync(deleteExactAsync(rec, tx));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> deleteExactAsync(@NotNull Tuple rec, @Nullable Transaction tx) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.deleteExact(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndDelete(@NotNull Tuple keyRec, @Nullable Transaction tx) {
        return sync(getAndDeleteAsync(keyRec, tx));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAndDeleteAsync(@NotNull Tuple keyRec, @Nullable Transaction tx) {
        Objects.requireNonNull(keyRec);

        final Row keyRow = marshal(keyRec, true);

        return tbl.getAndDelete(keyRow, (InternalTransaction) tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Tuple> deleteAll(@NotNull Collection<Tuple> keyRecs, @Nullable Transaction tx) {
        return sync(deleteAllAsync(keyRecs, tx));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> deleteAllAsync(@NotNull Collection<Tuple> keyRecs, @Nullable Transaction tx) {
        Objects.requireNonNull(keyRecs);

        return tbl.deleteAll(mapToBinary(keyRecs, true), (InternalTransaction) tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Tuple> deleteAllExact(@NotNull Collection<Tuple> recs, @Nullable Transaction tx) {
        return sync(deleteAllExactAsync(recs, tx));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> deleteAllExactAsync(@NotNull Collection<Tuple> recs, @Nullable Transaction tx) {
        Objects.requireNonNull(recs);

        return tbl.deleteAllExact(mapToBinary(recs, false), (InternalTransaction) tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override
    public <T extends Serializable> T invoke(
            @NotNull Tuple keyRec,
            InvokeProcessor<Tuple, Tuple, T> proc,
            @Nullable Transaction tx
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <T extends Serializable> CompletableFuture<T> invokeAsync(
            @NotNull Tuple keyRec,
            InvokeProcessor<Tuple, Tuple, T> proc,
            @Nullable Transaction tx
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public <T extends Serializable> Map<Tuple, T> invokeAll(
            @NotNull Collection<Tuple> keyRecs,
            InvokeProcessor<Tuple, Tuple, T> proc,
            @Nullable Transaction tx
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <T extends Serializable> CompletableFuture<Map<Tuple, T>> invokeAllAsync(
            @NotNull Collection<Tuple> keyRecs,
            InvokeProcessor<Tuple, Tuple, T> proc,
            @Nullable Transaction tx
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Marshal a tuple to a row.
     *
     * @param tuple   The tuple.
     * @param keyOnly Marshal key part only if {@code true}, otherwise marshal both, key and value parts.
     * @return Row.
     * @throws IgniteException If failed to marshal tuple.
     */
    private Row marshal(@NotNull Tuple tuple, boolean keyOnly) throws IgniteException {
        try {
            if (keyOnly) {
                return marsh.marshalKey(tuple);
            } else {
                return marsh.marshal(tuple);
            }
        } catch (TupleMarshallerException ex) {
            throw convertException(ex);
        }
    }

    /**
     * Returns table row tuple.
     *
     * @param row Binary row.
     */
    private Tuple wrap(BinaryRow row) {
        if (row == null) {
            return null;
        }

        final Row wrapped = schemaReg.resolve(row);

        return TableRow.tuple(wrapped);
    }

    /**
     * Returns table rows.
     *
     * @param rows Binary rows.
     */
    private Collection<Tuple> wrap(Collection<BinaryRow> rows) {
        if (rows == null) {
            return null;
        }

        return schemaReg.resolve(rows).stream().map(TableRow::tuple).collect(toList());
    }

    /**
     * Maps a collection of tuples to binary rows.
     *
     * @param rows Tuples.
     * @param key  {@code true} to marshal only a key.
     * @return List of binary rows.
     */
    private Collection<BinaryRow> mapToBinary(Collection<Tuple> rows, boolean key) {
        Collection<BinaryRow> mapped = new ArrayList<>(rows.size());

        for (Tuple row : rows) {
            mapped.add(marshal(row, key));
        }

        return mapped;
    }
}
