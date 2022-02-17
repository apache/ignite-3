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
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowPrefix;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.table.StorageRowListener;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableRow;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Internal index manager facade provides low-level methods for indexes operations.
 */
public class InternalSortedIndexImpl implements InternalSortedIndex, StorageRowListener {
    private final UUID id;

    private final String name;

    private final TableImpl tbl;

    private final SortedIndexStorage store;

    private final SortedIndexDescriptor desc;

    /**
     * Create sorted index.
     */
    public InternalSortedIndexImpl(UUID id, String name, SortedIndexStorage store, TableImpl tbl) {
        this.id = id;
        this.name = name;
        this.store = store;
        this.tbl = tbl;

        desc = store.indexDescriptor();
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public UUID tableId() {
        return tbl.tableId();
    }

    /** {@inheritDoc} */
    @Override
    public SortedIndexDescriptor descriptor() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<Tuple> scan(Tuple low, Tuple up, byte scanBoundMask, BitSet proj) {
        Cursor<IndexRow> cur = store.range(
                low != null ? new IndexRowPrefixTuple(low) {
                } : null,
                up != null ? new IndexRowPrefixTuple(up) : null,
                r -> true
        );

        List<String> tblColIds = new ArrayList<>(tbl.schemaView().schema().columnNames());
        Set<String> idxColIds = desc.columns().stream().map(SortedIndexColumnDescriptor::column).map(Column::name)
                .collect(Collectors.toSet());

        boolean needLookup = proj.stream().anyMatch(order -> !idxColIds.contains(tblColIds.get(order)));

        final TupleFactory tupleFactory = new TupleFactory(tbl.schemaView().lastSchemaVersion());

        return new IndexCursor(
                cur,
                needLookup ? r -> convertWithTableLookup(r, tupleFactory) : r -> convertIndexedOnly(r, tupleFactory)
        );
    }

    @Override
    public void drop() {
        tbl.internalTable().storage().dropIndex(name);
    }

    /** {@inheritDoc} */
    @Override
    public void onUpdate(@Nullable BinaryRow oldRow, BinaryRow newRow, int partId) {
        Tuple tupleNew = TableRow.tuple(tbl.schemaView().resolve(newRow));

        store.put(new IndexRowTuple(tupleNew, newRow.keyRow(), partId));

        if (oldRow != null) {
            Tuple tupleOld = TableRow.tuple(tbl.schemaView().resolve(oldRow));

            store.remove(new IndexRowTuple(tupleOld, oldRow.keyRow(), partId));
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onRemove(BinaryRow row, int partId) {
        Tuple t = TableRow.tuple(tbl.schemaView().resolve(row));

        store.remove(new IndexRowTuple(t, row.keyRow(), partId));
    }

    /**
     * Used for index only scan.
     */
    private Tuple convertIndexedOnly(IndexRow r, TupleFactory tupleFactory) {
        Tuple t = tupleFactory.create();

        for (int i = 0; i < desc.columns().size(); ++i) {
            t.set(desc.columns().get(i).column().name(), r.value(i));
        }

        return t;
    }

    /**
     * Additional lookup full row at the table by PK.
     */
    private Tuple convertWithTableLookup(IndexRow r, TupleFactory tupleFactory) {
        try {
            BinaryRow tblRow = tbl.internalTable().get(r.primaryKey(), null).get();

            Tuple tblTuple = TableRow.tuple(tbl.schemaView().resolve(tblRow));

            Tuple res = tupleFactory.create();

            for (int i = 0; i < res.columnCount(); ++i) {
                res.set(res.columnName(i), tblTuple.value(res.columnName(i)));
            }

            return res;
        } catch (Exception e) {
            throw new IgniteInternalException("Error on row lookup by index PK "
                    + "[index=" + name + ", indexRow=" + r + ']', e);
        }
    }

    /** Creates table tuple on specified schema. */
    private class TupleFactory {
        private final List<String> cols;

        TupleFactory(int tblSchemaVersion) {
            cols = Stream.concat(
                            Arrays.stream(tbl.schemaView().schema(tblSchemaVersion).keyColumns().columns()),
                            Arrays.stream(tbl.schemaView().schema(tblSchemaVersion).valueColumns().columns())
                    )
                    .sorted(Comparator.comparing(Column::columnOrder))
                    .map(Column::name)
                    .collect(Collectors.toList());
        }

        Tuple create() {
            Tuple t = Tuple.create();

            for (String colName : cols) {
                t.set(colName, null);
            }

            return t;
        }
    }

    private class IndexRowTuple implements IndexRow {
        private final Tuple tuple;

        private final BinaryRow pk;

        private final int part;

        IndexRowTuple(Tuple t, BinaryRow pk, int part) {
            this.tuple = t;
            this.pk = pk;
            this.part = part;
        }

        /** {@inheritDoc} */
        @Override
        public BinaryRow primaryKey() {
            return pk;
        }

        @Override
        public int partition() {
            return part;
        }

        /** {@inheritDoc} */
        @Override
        public Object value(int idxColOrder) {
            return tuple.value(desc.columns().get(idxColOrder).column().name());
        }
    }

    private static class IndexRowPrefixTuple implements IndexRowPrefix {
        private final Tuple tuple;

        IndexRowPrefixTuple(Tuple t) {
            this.tuple = t;
        }

        /** {@inheritDoc} */
        @Override
        public Object value(int idxColOrder) {
            return tuple.value(idxColOrder);
        }

        /** {@inheritDoc} */
        @Override
        public int length() {
            return tuple.columnCount();
        }
    }

    /**
     * Index store cursor adapter.
     */
    private static class IndexCursor implements Cursor<Tuple> {
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
    private static class IndexIterator implements Iterator<Tuple> {
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
