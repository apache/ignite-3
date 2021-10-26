package org.apache.ignite.internal.processors.query.calcite.extension;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;

/**
 * Asd.
 */
public class MyPhysTableScan extends TableScan implements IgniteRel {
    protected MyPhysTableScan(RelOptCluster cluster, RelTraitSet traitSet,
            List<RelHint> hints, RelOptTable table) {
        super(cluster, traitSet, hints, table);
    }

    /**
     * Asd.
     * @param input as.
     */
    public MyPhysTableScan(RelInput input) {
        super(changeTraits(input, MyConvention.INSTANCE));
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return (T) this;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new MyPhysTableScan(cluster, getTraitSet(), getHints(), getTable());
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }
}
