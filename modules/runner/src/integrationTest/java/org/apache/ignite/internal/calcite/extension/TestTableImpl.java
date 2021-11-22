package org.apache.ignite.internal.calcite.extension;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A test table implementation.
 */
class TestTableImpl extends AbstractTable implements IgniteTable {
    private final TableDescriptor desc;

    /**
     * Constructor.
     *
     * @param desc A descriptor of the table.
     */
    public TestTableImpl(TableDescriptor desc) {
        this.desc = desc;
    }

    /** {@inheritDoc} */
    @Override
    public TableDescriptor descriptor() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory, ImmutableBitSet requiredColumns) {
        return desc.rowType((IgniteTypeFactory) typeFactory, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override
    public Statistic getStatistic() {
        return new Statistic() {
            @Override
            public @Nullable List<RelCollation> getCollations() {
                return List.of();
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public TableScan toRel(RelOptCluster cluster, RelOptTable relOptTbl) {
        RelTraitSet traits = cluster.traitSetOf(TestExtension.CONVENTION)
                .replace(desc.distribution());

        return new TestPhysTableScan(
                cluster, traits, List.of(), relOptTbl
        );
    }

    /** {@inheritDoc} */
    @Override
    public IgniteDistribution distribution() {
        return desc.distribution();
    }

    /** {@inheritDoc} */
    @Override
    public <C> C unwrap(Class<C> cls) {
        if (cls.isInstance(desc)) {
            return cls.cast(desc);
        }

        return super.unwrap(cls);
    }
}
