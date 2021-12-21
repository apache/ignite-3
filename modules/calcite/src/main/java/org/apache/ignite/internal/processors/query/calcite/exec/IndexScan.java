package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.idx.InternalSortedIndex;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteException;

/**
 * Scan on index.
 */
public class IndexScan<RowT> extends AbstractIndexScan<RowT, IndexRow> {
    private final IgniteIndex idx;

    private final ImmutableBitSet requiredColumns;

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
                new TreeIndexWrapper(idx.index()),
                filters,
                lower,
                upper,
                rowTransformer
        );

        this.idx = idx;
        this.requiredColumns = requiredColumns;
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
    protected IndexRow row2indexRow(RowT bound) {
        if (bound == null) {
            return null;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override
    protected RowT indexRow2Row(IndexRow row) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
    }

    /**
     * Sorted index wrapper.
     */
    private static class TreeIndexWrapper implements TreeIndex<IndexRow> {
        /** Underlying index. */
        private final InternalSortedIndex idx;

        /**
         * Creates wrapper for InternalSortedIndex.
         */
        private TreeIndexWrapper(InternalSortedIndex idx) {
            this.idx = idx;
        }

        /** {@inheritDoc} */
        @Override
        public Cursor<IndexRow> find(IndexRow lower, IndexRow upper) {
            try {
                // return idx.scan(lower, upper, 0, requiredColumns.toBitSet());
                return null;
            } catch (Exception e) {
                throw new IgniteException("Failed to find index rows", e);
            }
        }
    }
}
