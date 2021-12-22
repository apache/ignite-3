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

package org.apache.ignite.internal.idx;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.storage.index.IndexBinaryRow;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.SortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.table.StorageRowListener;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableRow;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Internal index manager facade provides low-level methods for indexes operations.
 */
public class InternalSortedIndexImpl implements InternalSortedIndex, StorageRowListener {
    private final IgniteUuid id;

    private final String name;

    private final TableImpl tbl;

    private final SortedIndexStorage store;


    /**
     * Create sorted index.
     */
    public InternalSortedIndexImpl(IgniteUuid id, String name, SortedIndexStorage store, TableImpl tbl) {
        this.id = id;
        this.name = name;
        this.store = store;
        this.tbl = tbl;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override
    public String tableName() {
        return tbl.name();
    }

    /** {@inheritDoc} */
    @Override
    public List<Column> columns() {
        return store.indexDescriptor().columns().stream()
                .map(SortedIndexColumnDescriptor::column)
                .collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<Tuple> scan(Tuple low, Tuple up, byte scanBoundMask, BitSet proj) {
        Cursor<IndexRow> cur = store.range(
                low != null ? new IndexSearchRow(low) : null,
                up != null ? new IndexSearchRow(up) : null,
                r -> true
        );

        List<String> tblColIds = new ArrayList<>(tbl.schemaView().schema().columnNames());
        Set<String> idxColIds = columns().stream().map(Column::name).collect(Collectors.toSet());

        boolean needLookup = proj.stream().anyMatch(order -> !idxColIds.contains(tblColIds.get(order)));

        return new IndexCursor(cur, needLookup ? this::convertWithTableLookup : this::convertIndexedOnly);
    }

    @Override
    public void drop() {
        store.destroy();
    }

    /** {@inheritDoc} */
    @Override
    public void onUpdate(@Nullable BinaryRow oldRow, BinaryRow newRow, int partId) {
        Tuple t = TableRow.tuple(tbl.schemaView().resolve(newRow));

        IndexBinaryRow idxBinRow = store.indexRowFactory().createIndexRow(t, newRow.keyRow(), partId);

        store.put(idxBinRow);
    }

    /** {@inheritDoc} */
    @Override
    public void onRemove(BinaryRow row) {
        Tuple t = TableRow.tuple(tbl.schemaView().resolve(row));

        IndexBinaryRow idxBinRow = store.indexRowFactory().createIndexRow(t, row.keyRow(), -1);

        store.remove(idxBinRow);
    }

    /**
     * Used for index only scan.
     */
    private Tuple convertIndexedOnly(IndexRow r) {
        assert r.columnsCount() == store.indexDescriptor().columns().size() : "Unexpected Index row to convert [idxRow=" + r
                + ", index=" + this + ']';

        Tuple t = Tuple.create();

        tbl.schemaView().schema().columnNames().forEach(colName -> t.set(colName, null));

        for (int i = 0; i < r.columnsCount(); ++i) {
            t.set(store.indexDescriptor().columns().get(i).column().name(), r.value(i));
        }

        return t;
    }

    /**
     * Additional lookup full row at the table by PK.
     */
    private Tuple convertWithTableLookup(IndexRow r) {
        try {
            BinaryRow tblRow = tbl.internalTable().get(r.primaryKey(), null).get();

            return TableRow.tuple(tbl.schemaView().resolve(tblRow));
        } catch (Exception e) {
            throw new IgniteInternalException("Error on row lookup by index PK "
                    + "[index=" + name + ", indexRow=" + r + ']', e);
        }
    }

    private class IndexSearchRow implements IndexRow {
        private final Tuple tuple;

        IndexSearchRow(Tuple t) {
            this.tuple = t;
        }

        /** {@inheritDoc} */
        @Override
        public byte[] rowBytes() {
            return null;
        }

        /** {@inheritDoc} */
        @Override
        public BinaryRow primaryKey() {
            return null;
        }

        /** {@inheritDoc} */
        @Override
        public Object value(int idxColOrder) {
            return tuple.value(idxColOrder);
        }

        /** {@inheritDoc} */
        @Override
        public int columnsCount() {
            return tuple.columnCount();
        }
    }

    /**
     * Index store cursor adapter.
     */
    private class IndexCursor implements Cursor<Tuple> {
        private final Cursor<IndexRow> cur;
        private final IndexIterator it;

        IndexCursor(Cursor<IndexRow> cur, Function<IndexRow, Tuple> rowConverter) {
            this.cur = cur;
            it = new IndexIterator(cur.iterator(), rowConverter);
        }

        /** {@inheritDoc} */
        @Override
        public void close() throws Exception {
            cur.close();
        }

        /** {@inheritDoc} */
        @NotNull
        @Override
        public Iterator<Tuple> iterator() {
            return it;
        }

        /** {@inheritDoc} */
        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        /** {@inheritDoc} */
        @Override
        public Tuple next() {
            return it.next();
        }
    }

    /**
     * Index store iterator adapter.
     */
    private class IndexIterator implements Iterator<Tuple> {
        private final Iterator<IndexRow> it;

        private final Function<IndexRow, Tuple> rowConverter;

        private IndexIterator(Iterator<IndexRow> it, Function<IndexRow, Tuple> rowConverter) {
            this.it = it;
            this.rowConverter = rowConverter;
        }

        /** {@inheritDoc} */
        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        /** {@inheritDoc} */
        @Override
        public Tuple next() {
            IndexRow r = it.next();

            return rowConverter.apply(r);
        }
    }
}
