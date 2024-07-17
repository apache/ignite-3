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

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCost;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.sql.engine.util.RexUtils;

/**
 * Relational operator that returns the hashed contents of a table and allow to lookup rows by specified keys.
 */
public class IgniteHashIndexSpool extends AbstractIgniteSpool {
    private static final String REL_TYPE_NAME = "HashIndexSpool";

    /** Search row. */
    private final List<RexNode> searchRow;

    /** Keys (number of the columns at the input row) to build hash index. */
    private final ImmutableBitSet keys;

    /** Condition (used to calculate selectivity). */
    private final RexNode cond;

    /** Allow NULL values. */
    private final boolean allowNulls;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public IgniteHashIndexSpool(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            List<RexNode> searchRow,
            RexNode cond,
            boolean allowNulls
    ) {
        super(cluster, traits, Type.LAZY, input);

        assert !nullOrEmpty(searchRow);

        this.searchRow = searchRow;
        this.cond = cond;
        this.allowNulls = allowNulls;

        keys = ImmutableBitSet.of(RexUtils.notNullKeys(searchRow));
    }

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteHashIndexSpool(RelInput input) {
        this(input.getCluster(),
                input.getTraitSet().replace(IgniteConvention.INSTANCE),
                input.getInputs().get(0),
                input.getExpressionList("searchRow"),
                input.getExpression("condition"),
                input.getBoolean("allowNulls", false)
        );
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public RelNode accept(RexShuttle shuttle) {
        shuttle.apply(cond);

        return super.accept(shuttle);
    }

    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteHashIndexSpool(cluster, getTraitSet(), inputs.get(0), searchRow, cond, allowNulls);
    }

    /** {@inheritDoc} */
    @Override
    protected Spool copy(RelTraitSet traitSet, RelNode input, Type readType, Type writeType) {
        return new IgniteHashIndexSpool(getCluster(), traitSet, input, searchRow, cond, allowNulls);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isEnforcer() {
        return true;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        RelWriter writer = super.explainTerms(pw);

        return writer
                .item("searchRow", searchRow)
                .item("condition", cond)
                .item("allowNulls", allowNulls);
    }

    /** {@inheritDoc} */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCnt = mq.getRowCount(getInput());
        double bytesPerRow = getRowType().getFieldCount() * IgniteCost.AVERAGE_FIELD_SIZE;
        double totalBytes = rowCnt * bytesPerRow;
        double cpuCost = IgniteCost.HASH_LOOKUP_COST;

        IgniteCostFactory costFactory = (IgniteCostFactory) planner.getCostFactory();

        return costFactory.makeCost(rowCnt, cpuCost, 0, totalBytes, 0);
    }

    /** {@inheritDoc} */
    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return mq.getRowCount(getInput()) * mq.getSelectivity(this, null);
    }

    /**
     * Get search row.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public List<RexNode> searchRow() {
        return searchRow;
    }

    /**
     * Get keys.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public ImmutableBitSet keys() {
        return keys;
    }

    /**
     * Get condition.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public RexNode condition() {
        return cond;
    }

    /** Allow {@code null} comparison as equal, use in case of NOT DISTINCT FROM syntax. */
    public boolean allowNulls() {
        return allowNulls;
    }

    /** {@inheritDoc} */
    @Override
    public String getRelTypeName() {
        return REL_TYPE_NAME;
    }
}
