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

package org.apache.ignite.internal.sql.engine.exec;

import java.util.BitSet;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.idx.InternalSortedIndex;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.Tuple;

/**
 * Scan on index.
 */
public class IndexScan<RowT> extends AbstractIndexScan<RowT, Tuple> {
    private final IgniteIndex idx;

    private final ImmutableBitSet requiredColumns;

    private final RowFactory<RowT> factory;

    /**
     * Creates index scan.
     */
    public IndexScan(
            IgniteIndex idx,
            ExecutionContext<RowT> ectx,
            ColocationGroup colocationGrp,
            Predicate<RowT> filters,
            Supplier<RowT> lower,
            Supplier<RowT> upper,
            Function<RowT, RowT> rowTransformer,
            ImmutableBitSet requiredColumns
    ) {
        super(
                ectx,
                idx.table().getRowType(ectx.getTypeFactory(), requiredColumns),
                new TreeIndexWrapper(idx.index(), requiredColumns.toBitSet()),
                filters,
                lower,
                upper,
                rowTransformer
        );

        this.idx = idx;
        this.requiredColumns = requiredColumns;
        factory = ectx.rowHandler().factory(ectx.getTypeFactory(), rowType);
    }

    /** {@inheritDoc} */
    @Override
    public synchronized Iterator<RowT> iterator() {
        try {
            return super.iterator();
        } catch (Exception e) {
            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override
    protected Tuple row2indexRow(RowT bound) {
        if (bound == null) {
            return null;
        }

        RowHandler<RowT> hnd = ectx.rowHandler();

        Tuple t = Tuple.create();

        for (int i = 0; i < hnd.columnCount(bound); ++i) {
            t.set("idx" + i, hnd.get(i, bound));
        }

        return t;
    }

    /** {@inheritDoc} */
    @Override
    protected RowT indexRow2Row(Tuple t) {
        RowT row = factory.create();

        RowHandler<RowT> hnd = ectx.rowHandler();

        for (int i = 0; i < t.columnCount(); ++i) {
            hnd.set(i, row, t.value(i));
        }

        return row;
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
    }

    /**
     * Sorted index wrapper.
     */
    private static class TreeIndexWrapper implements TreeIndex<Tuple> {
        /** Underlying index. */
        private final InternalSortedIndex idx;

        private final BitSet requiredColumns;

        /**
         * Creates wrapper for InternalSortedIndex.
         */
        private TreeIndexWrapper(InternalSortedIndex idx, BitSet requiredColumns) {
            this.idx = idx;
            this.requiredColumns = requiredColumns;
        }

        /** {@inheritDoc} */
        @Override
        public Cursor<Tuple> find(Tuple lower, Tuple upper) {
            try {
                return idx.scan(
                        lower,
                        upper,
                        (byte) (InternalSortedIndex.GREATER_OR_EQUAL | InternalSortedIndex.LESS_OR_EQUAL),
                        requiredColumns
                );
            } catch (Exception e) {
                throw new IgniteException("Failed to find index rows", e);
            }
        }
    }
}
