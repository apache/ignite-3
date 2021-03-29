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
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Row;
import org.apache.ignite.internal.schema.RowAssembler;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.lang.IgniteFuture;
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
public class TableImpl implements Table {
    /** Table. */
    private final InternalTable tbl;

    /** Schema manager. */
    private final TableSchemaManager schemaMgr;

    /**
     * Constructor.
     *
     * @param tbl Table.
     */
    public TableImpl(InternalTable tbl, TableSchemaManager schemaMgr) {
        this.tbl = tbl;
        this.schemaMgr = schemaMgr;
    }

    /** {@inheritDoc} */
    @Override public <R> RecordView<R> recordView(RecordMapper<R> recMapper) {
        return new RecordViewImpl<>(tbl, schemaMgr, recMapper);
    }

    /** {@inheritDoc} */
    @Override public <K, V> KeyValueView<K, V> kvView(KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        return new KVViewImpl<>(tbl, schemaMgr, keyMapper, valMapper);
    }

    /** {@inheritDoc} */
    @Override public KeyValueBinaryView kvView() {
        return new KeyValueBinaryViewImpl(tbl, schemaMgr);
    }

    /** {@inheritDoc} */
    @Override public Tuple get(Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        try {
            final Row keyRow = marshallToRow(keyRec, true); // Convert to portable format to pass TX/storage layer.

            return tbl.get(keyRow).thenApply(this::wrap).get();
        }
        catch (Exception e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Tuple> getAsync(Tuple keyRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> getAll(Collection<Tuple> keyRecs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Collection<Tuple>> getAllAsync(Collection<Tuple> keyRecs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void upsert(Tuple rec) {
        Objects.requireNonNull(rec);

        try {
            final Row keyRow = marshallToRow(rec, false);

            tbl.upsert(keyRow).get();
        }
        catch (Exception e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Void> upsertAsync(Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void upsertAll(Collection<Tuple> recs) {

    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Void> upsertAllAsync(Collection<Tuple> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndUpsert(Tuple rec) {
        Objects.requireNonNull(rec);

        try {
            final Row keyRow = marshallToRow(rec, false);

            return tbl.getAndUpsert(keyRow).thenApply(this::wrap).get();
        }
        catch (Exception e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Tuple> getAndUpsertAsync(Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean insert(Tuple rec) {
        Objects.requireNonNull(rec);

        try {
            final Row keyRow = marshallToRow(rec, false);

            return tbl.insert(keyRow).get();
        }
        catch (Exception e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> insertAsync(Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> insertAll(Collection<Tuple> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Collection<Tuple>> insertAllAsync(Collection<Tuple> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(Tuple rec) {
        Objects.requireNonNull(rec);

        try {
            final Row keyRow = marshallToRow(rec, false);

            return tbl.replace(keyRow).get();
        }
        catch (Exception e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> replaceAsync(Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(Tuple oldRec, Tuple newRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> replaceAsync(Tuple oldRec, Tuple newRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndReplace(Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Tuple> getAndReplaceAsync(Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean delete(Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        try {
            final Row keyRow = marshallToRow(keyRec, false);

            return tbl.delete(keyRow).get();
        }
        catch (Exception e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> deleteAsync(Tuple keyRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean deleteExact(Tuple rec) {
        Objects.requireNonNull(rec);

        try {
            final Row row = marshallToRow(rec, false);

            return tbl.deleteExact(row).get();
        }
        catch (Exception e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> deleteExactAsync(Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndDelete(Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Tuple> getAndDeleteAsync(Tuple rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> deleteAll(Collection<Tuple> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Collection<Tuple>> deleteAllAsync(Collection<Tuple> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> deleteAllExact(Collection<Tuple> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Collection<Tuple>> deleteAllExactAsync(
        Collection<Tuple> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> T invoke(
        Tuple keyRec,
        InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> IgniteFuture<T> invokeAsync(
        Tuple keyRec,
        InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> Map<Tuple, T> invokeAll(
        Collection<Tuple> keyRecs,
        InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> IgniteFuture<Map<Tuple, T>> invokeAllAsync(
        Collection<Tuple> keyRecs,
        InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public TupleBuilder tupleBuilder() {
        return new TupleBuilderImpl();
    }

    /**
     * @param rec Record tuple.
     * @param skipValue Skip value columns.
     * @return Table row.
     */
    //TODO: Move to marshaller.
    private Row marshallToRow(Tuple rec, boolean skipValue) {
        final SchemaDescriptor schema = schemaMgr.schema();

        assert rec instanceof TupleBuilderImpl;

        final RowAssembler rowBuilder = new RowAssembler(schema, 4096, 0, 0);

        for (int i = 0; i < schema.keyColumns().length(); i++) {
            final Column col = schema.keyColumns().column(i);

            writeColumn(rec, col, rowBuilder);
        }

        if (!skipValue) {
            for (int i = 0; i < schema.valueColumns().length(); i++) {
                final Column col = schema.valueColumns().column(i);

                writeColumn(rec, col, rowBuilder);
            }
        }

        return new Row(schema, new ByteBufferRow(rowBuilder.build()));
    }

    //TODO: Move to marshaller.
    private void writeColumn(Tuple rec, Column col, RowAssembler rowWriter) {
        if (rec.value(col.name()) == null) {
            rowWriter.appendNull();
            return;
        }

        switch (col.type().spec()) {
            case LONG: {
                rowWriter.appendLong(rec.longValue(col.name()));

                break;
            }

            default:
                throw new IllegalStateException("Unexpected value: " + col.type());
        }
    }

    /**
     * @param row Binary row.
     * @return Table row.
     */
    private TableRow wrap(BinaryRow row) {
        if (row == null)
            return null;

        final SchemaDescriptor schema = schemaMgr.schema(row.schemaVersion());

        return new TableRow(schema, new Row(schema, row));
    }

    /**
     * @param e Exception.
     * @return Runtime exception.
     */
    private RuntimeException convertException(Exception e) {
        if (e instanceof InterruptedException)
            Thread.currentThread().interrupt(); // Restore interrupt flag.

        return new RuntimeException(e);
    }
}
