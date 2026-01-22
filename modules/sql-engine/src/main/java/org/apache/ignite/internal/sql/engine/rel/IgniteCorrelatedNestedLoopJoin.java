/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.rel;

import static org.apache.ignite.internal.sql.engine.util.Commons.maxPrefix;

import it.unimi.dsi.fastutil.ints.IntList;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCost;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.sql.engine.rel.explain.IgniteRelWriter;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * Relational expression that combines two relational expressions according to some condition.
 *
 * <p>Each output row has columns from the left and right inputs.
 * The set of output rows is a subset of the cartesian product of the two inputs; precisely which subset depends on the join condition.
 */
public class IgniteCorrelatedNestedLoopJoin extends AbstractIgniteJoin {
    private static final String REL_TYPE_NAME = "CorrelatedNestedLoopJoin";

    private final ImmutableBitSet correlationColumns;

    /**
     * Creates a Join.
     *
     * @param cluster Cluster
     * @param traitSet Trait set
     * @param left Left input
     * @param right Right input
     * @param condition Join condition
     * @param variablesSet Set variables that are set by the LHS and used by the RHS and are not available to nodes above this Join
     *         in the tree
     * @param joinType Join type
     * @param correlationColumns Set of columns that are used by correlation.
     */
    public IgniteCorrelatedNestedLoopJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right,
            RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType, ImmutableBitSet correlationColumns) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);

        this.correlationColumns = correlationColumns;
    }

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public IgniteCorrelatedNestedLoopJoin(RelInput input) {
        this(input.getCluster(),
                input.getTraitSet().replace(IgniteConvention.INSTANCE),
                input.getInputs().get(0),
                input.getInputs().get(1),
                input.getExpression("condition"),
                Set.copyOf(Commons.transform(input.getIntegerList("variablesSet"), CorrelationId::new)),
                input.getEnum("joinType", JoinRelType.class),
                input.getBitSet("correlationColumns"));
    }

    /** {@inheritDoc} */
    @Override
    public Join copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType,
            boolean semiJoinDone) {
        return new IgniteCorrelatedNestedLoopJoin(getCluster(), traitSet, left, right, condition, variablesSet, joinType,
                correlationColumns);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(
            RelTraitSet nodeTraits,
            List<RelTraitSet> inputTraits
    ) {
        RelTraitSet left = inputTraits.get(0);
        RelTraitSet right = inputTraits.get(1);
        RelCollation leftCollation = TraitUtils.collation(left);
        RelCollation rightCollation = TraitUtils.collation(right);

        IntList newRightCollationFields = maxPrefix(rightCollation.getKeys(), joinInfo.leftKeys);

        if (newRightCollationFields.isEmpty()) {
            return List.of(Pair.of(nodeTraits.replace(RelCollations.EMPTY), inputTraits));
        }

        // We preserve left edge collation only if batch size == 1
        if (variablesSet.size() == 1) {
            nodeTraits = nodeTraits.replace(leftCollation);
        } else {
            nodeTraits = nodeTraits.replace(RelCollations.EMPTY);
        }

        return List.of(Pair.of(nodeTraits, inputTraits));
    }

    /** {@inheritDoc} */
    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughCollation(
            RelTraitSet nodeTraits,
            List<RelTraitSet> inputTraits
    ) {
        RelTraitSet left = inputTraits.get(0);
        RelTraitSet right = inputTraits.get(1);

        // We preserve left edge collation only if batch size == 1
        if (variablesSet.size() == 1) {
            Pair<RelTraitSet, List<RelTraitSet>> baseTraits = super.passThroughCollation(nodeTraits, inputTraits);

            return Pair.of(
                    baseTraits.getKey(),
                    List.of(
                            baseTraits.getValue().get(0),
                            baseTraits.getValue().get(1)
                    )
            );
        }

        return Pair.of(nodeTraits.replace(RelCollations.EMPTY),
                List.of(left.replace(RelCollations.EMPTY), right));
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
        // TODO use super.explainTerms(...)
        return super.explainTerms(pw)
                .item("condition", condition)
                .item("joinType", joinType.lowerName)
                .itemIf("correlationColumns", correlationColumns, !correlationColumns.isEmpty())
                .itemIf("variablesSet", variablesSet, !variablesSet.isEmpty())
                .itemIf(
                        "systemFields",
                        getSystemFieldList(),
                        !getSystemFieldList().isEmpty());
    }

    /** {@inheritDoc} */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory) planner.getCostFactory();

        double leftCount = mq.getRowCount(getLeft());

        if (Double.isInfinite(leftCount)) {
            return costFactory.makeInfiniteCost();
        }

        double rightCount = mq.getRowCount(getRight());

        if (Double.isInfinite(rightCount)) {
            return costFactory.makeInfiniteCost();
        }

        double rows = leftCount * rightCount;

        return costFactory.makeCost(rows,
                rows * (IgniteCost.ROW_COMPARISON_COST + IgniteCost.ROW_PASS_THROUGH_COST), 0);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteCorrelatedNestedLoopJoin(cluster, getTraitSet(), inputs.get(0), inputs.get(1), getCondition(),
                getVariablesSet(), getJoinType(), getCorrelationColumns());
    }

    public ImmutableBitSet getCorrelationColumns() {
        return correlationColumns;
    }

    /** {@inheritDoc} */
    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        // condition selectivity already counted within the external filter
        return super.estimateRowCount(mq) / mq.getSelectivity(this, getCondition());
    }

    /** {@inheritDoc} */
    @Override
    public String getRelTypeName() {
        return REL_TYPE_NAME;
    }

    @Override
    public IgniteRelWriter explain(IgniteRelWriter writer) {
        return super.explain(writer).addCorrelatedVariables(variablesSet);
    }
}
