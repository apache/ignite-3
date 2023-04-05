package org.apache.ignite.internal.sql.engine.planner.util;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Flow.Publisher;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.index.ColumnCollation;
import org.apache.ignite.internal.index.SortedIndex;
import org.apache.ignite.internal.index.SortedIndexDescriptor;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

public class TestSortedIndex implements SortedIndex {
    private final UUID id = UUID.randomUUID();

    private final UUID tableId = UUID.randomUUID();

    private final SortedIndexDescriptor descriptor;

    public static TestSortedIndex create(RelCollation collation, String name, IgniteTable table) {
        List<String> columns = new ArrayList<>();
        List<ColumnCollation> collations = new ArrayList<>();
        TableDescriptor tableDescriptor = table.descriptor();

        for (var fieldCollation : collation.getFieldCollations()) {
            columns.add(tableDescriptor.columnDescriptor(fieldCollation.getFieldIndex()).name());
            collations.add(ColumnCollation.get(
                    !fieldCollation.getDirection().isDescending(),
                    fieldCollation.nullDirection == NullDirection.FIRST
            ));
        }

        var descriptor = new SortedIndexDescriptor(name, columns, collations);

        return new TestSortedIndex(descriptor);
    }

    public TestSortedIndex(SortedIndexDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    /** {@inheritDoc} */
    @Override
    public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return descriptor.name();
    }

    /** {@inheritDoc} */
    @Override
    public UUID tableId() {
        return tableId;
    }

    /** {@inheritDoc} */
    @Override
    public SortedIndexDescriptor descriptor() {
        return descriptor;
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<BinaryRow> lookup(int partId, UUID txId, PrimaryReplica recipient, BinaryTuple key,
            @Nullable BitSet columns) {
        throw new AssertionError("Should not be called");
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<BinaryRow> lookup(int partId, HybridTimestamp timestamp, ClusterNode recipient, BinaryTuple key, BitSet columns) {
        throw new AssertionError("Should not be called");
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<BinaryRow> scan(int partId, HybridTimestamp timestamp, ClusterNode recipient,
            @Nullable BinaryTuplePrefix leftBound, @Nullable BinaryTuplePrefix rightBound, int flags, BitSet columnsToInclude) {
        throw new AssertionError("Should not be called");
    }

    @Override
    public Publisher<BinaryRow> scan(int partId, UUID txId, PrimaryReplica recipient, @Nullable BinaryTuplePrefix leftBound,
            @Nullable BinaryTuplePrefix rightBound, int flags, @Nullable BitSet columnsToInclude) {
        throw new AssertionError("Should not be called");
    }
}
