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

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleBuilder;
import org.apache.ignite.table.mapper.KeyMapper;
import org.apache.ignite.table.mapper.RecordMapper;
import org.apache.ignite.table.mapper.ValueMapper;
import org.jetbrains.annotations.NotNull;

/**
 * Table view implementation for binary objects.
 */
public class TableImpl extends AbstractTableView implements Table {
    /** Marshaller. */
    private final TupleMarshallerImpl marsh;

    /**
     * Constructor.
     *
     * @param tbl Table.
     * @param schemaReg Table schema registry.
     */
    public TableImpl(InternalTable tbl, SchemaRegistry schemaReg) {
        super(tbl, schemaReg);

        marsh = new TupleMarshallerImpl(schemaReg);
    }

    /**
     * Gets a table id.
     *
     * @return Table id as UUID.
     */
    public @NotNull UUID tableId() {
        return tbl.tableId();
    }

    /** {@inheritDoc} */
    @Override public @NotNull String tableName() {
        return tbl.tableName();
    }

    /**
     * Gets a schema view for the table.
     *
     * @return Schema view.
     */
    public SchemaRegistry schemaView() {
        return schemaReg;
    }

    /** {@inheritDoc} */
    @Override public <R> RecordView<R> recordView(RecordMapper<R> recMapper) {
        return new RecordViewImpl<>(tbl, schemaReg, recMapper);
    }

    /** {@inheritDoc} */
    @Override public <K, V> KeyValueView<K, V> kvView(KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        return new KVViewImpl<>(tbl, schemaReg, keyMapper, valMapper);
    }

    /** {@inheritDoc} */
    @Override public KeyValueBinaryView kvView() {
        return new KVBinaryViewImpl(tbl, schemaReg);
    }

    /** {@inheritDoc} */
    @Override public Tuple get(@NotNull Tuple keyRec) {
        return sync(getAsync(keyRec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAsync(@NotNull Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        final Row keyRow = marshaller().marshal(keyRec, null); // Convert to portable format to pass TX/storage layer.

        return tbl.get(keyRow).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> getAll(@NotNull Collection<Tuple> keyRecs) {
        return sync(getAllAsync(keyRecs));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> getAllAsync(@NotNull Collection<Tuple> keyRecs) {
        Objects.requireNonNull(keyRecs);

        HashSet<BinaryRow> keys = new HashSet<>(keyRecs.size());

        for (Tuple keyRec : keyRecs) {
            final Row keyRow = marshaller().marshal(keyRec, null);

            keys.add(keyRow);
        }

        return tbl.getAll(keys).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override public void upsert(@NotNull Tuple rec) {
        sync(upsertAsync(rec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> upsertAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row keyRow = marshaller().marshal(rec);

        return tbl.upsert(keyRow);
    }

    /** {@inheritDoc} */
    @Override public void upsertAll(@NotNull Collection<Tuple> recs) {
        sync(upsertAllAsync(recs));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> upsertAllAsync(@NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        HashSet<BinaryRow> keys = new HashSet<>(recs.size());

        for (Tuple keyRec : recs) {
            final Row keyRow = marshaller().marshal(keyRec);

            keys.add(keyRow);
        }

        return tbl.upsertAll(keys);
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndUpsert(@NotNull Tuple rec) {
        return sync(getAndUpsertAsync(rec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndUpsertAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row keyRow = marshaller().marshal(rec);

        return tbl.getAndUpsert(keyRow).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override public boolean insert(@NotNull Tuple rec) {
        return sync(insertAsync(rec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> insertAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row keyRow = marshaller().marshal(rec);

        return tbl.insert(keyRow);
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> insertAll(@NotNull Collection<Tuple> recs) {
        return sync(insertAllAsync(recs));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> insertAllAsync(@NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        HashSet<BinaryRow> keys = new HashSet<>(recs.size());

        for (Tuple keyRec : recs) {
            final Row keyRow = marshaller().marshal(keyRec);

            keys.add(keyRow);
        }

        return tbl.insertAll(keys).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull Tuple rec) {
        return sync(replaceAsync(rec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row keyRow = marshaller().marshal(rec);

        return tbl.replace(keyRow);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull Tuple oldRec, @NotNull Tuple newRec) {
        return sync(replaceAsync(oldRec, newRec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple oldRec, @NotNull Tuple newRec) {
        Objects.requireNonNull(oldRec);
        Objects.requireNonNull(newRec);

        final Row oldRow = marshaller().marshal(oldRec);
        final Row newRow = marshaller().marshal(newRec);

        return tbl.replace(oldRow, newRow);
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndReplace(@NotNull Tuple rec) {
        return sync(getAndReplaceAsync(rec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndReplaceAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row keyRow = marshaller().marshal(rec);

        return tbl.getAndReplace(keyRow).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override public boolean delete(@NotNull Tuple keyRec) {
        return sync(deleteAsync(keyRec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> deleteAsync(@NotNull Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        final Row keyRow = marshaller().marshal(keyRec, null);

        return tbl.delete(keyRow);
    }

    /** {@inheritDoc} */
    @Override public boolean deleteExact(@NotNull Tuple rec) {
        return sync(deleteExactAsync(rec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> deleteExactAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row row = marshaller().marshal(rec);

        return tbl.deleteExact(row);
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndDelete(@NotNull Tuple rec) {
        return sync(getAndDeleteAsync(rec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndDeleteAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row row = marshaller().marshal(rec);

        return tbl.getAndDelete(row).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> deleteAll(@NotNull Collection<Tuple> recs) {
        return sync(deleteAllAsync(recs));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> deleteAllAsync(@NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        HashSet<BinaryRow> keys = new HashSet<>(recs.size());

        for (Tuple keyRec : recs) {
            final Row keyRow = marshaller().marshal(keyRec, null);

            keys.add(keyRow);
        }

        return tbl.deleteAll(keys).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> deleteAllExact(@NotNull Collection<Tuple> recs) {
        return sync(deleteAllExactAsync(recs));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> deleteAllExactAsync(
        @NotNull Collection<Tuple> recs
    ) {
        Objects.requireNonNull(recs);

        HashSet<BinaryRow> keys = new HashSet<>(recs.size());

        for (Tuple keyRec : recs) {
            final Row keyRow = marshaller().marshal(keyRec);

            keys.add(keyRow);
        }

        return tbl.deleteAllExact(keys).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> T invoke(
        @NotNull Tuple keyRec,
        InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> CompletableFuture<T> invokeAsync(
        @NotNull Tuple keyRec,
        InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> Map<Tuple, T> invokeAll(
        @NotNull Collection<Tuple> keyRecs,
        InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> CompletableFuture<Map<Tuple, T>> invokeAllAsync(
        @NotNull Collection<Tuple> keyRecs,
        InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public TupleBuilder tupleBuilder() {
        return new TupleBuilderImpl(schemaReg.schema());
    }

    /**
     * @return Marshaller.
     */
    private TupleMarshaller marshaller() {
        return marsh;
    }

    /**
     * @param row Binary row.
     * @return Table row.
     */
    private TableRow wrap(BinaryRow row) {
        if (row == null)
            return null;

        final SchemaDescriptor schema = schemaReg.schema(row.schemaVersion());

        return new TableRow(schema, new Row(schema, row));
    }

    /**
     * @param rows Binary rows.
     * @return Table rows.
     */
    private Collection<Tuple> wrap(Collection<BinaryRow> rows) {
        if (rows == null)
            return null;

        return rows.stream().map(this::wrap).collect(Collectors.toSet());
    }
}
