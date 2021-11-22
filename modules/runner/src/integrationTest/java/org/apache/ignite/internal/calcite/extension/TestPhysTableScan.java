package org.apache.ignite.internal.calcite.extension;

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;

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

/**
 * A scan over test table.
 */
public class TestPhysTableScan extends TableScan implements IgniteRel {
    /**
     * Constructor.
     *
     * @param cluster  Cluster this node belongs to.
     * @param traitSet A set of the traits this node satisfy.
     * @param hints    A list of hints applicable to the current node.
     * @param table    The actual table to be scanned.
     */
    protected TestPhysTableScan(RelOptCluster cluster, RelTraitSet traitSet,
            List<RelHint> hints, RelOptTable table) {
        super(cluster, traitSet, hints, table);
    }

    /**
     * Constructor.
     *
     * @param input Context to recover this relation from.
     */
    public TestPhysTableScan(RelInput input) {
        super(changeTraits(input, TestExtension.CONVENTION));
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return (T) this;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new TestPhysTableScan(cluster, getTraitSet(), getHints(), getTable());
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }
}
