package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.BitSet;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.idx.InternalSortedIndex;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
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
    protected RowT indexRow2Row(Tuple row) {
        return idx.table().toRow(ectx, row, factory, requiredColumns);
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
