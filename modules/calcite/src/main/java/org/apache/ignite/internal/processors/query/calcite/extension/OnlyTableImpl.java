package org.apache.ignite.internal.processors.query.calcite.extension;

import java.util.List;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.random;

/**
 * Asd.
 */
class OnlyTableImpl extends AbstractTable implements IgniteTable {
    private final TableDescriptor desc;

    /**
     * Asd.
     * @param desc Asd.
     */
    public OnlyTableImpl(TableDescriptor desc) {
        this.desc = desc;
    }

    @Override
    public TableDescriptor descriptor() {
        return desc;
    }

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

    @Override
    public TableScan toRel(RelOptCluster cluster, RelOptTable relOptTbl) {
        RelTraitSet traits = cluster.traitSetOf(MyConvention.INSTANCE)
                .replace(IgniteDistributions.random());

        return new MyPhysTableScan(
                cluster, traits, List.of(), relOptTbl
        );
    }

    @Override
    public IgniteDistribution distribution() {
        return descriptor().distribution();
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
