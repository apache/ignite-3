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

import java.util.List;
import java.util.Objects;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.ignite.internal.sql.engine.externalize.RelInputEx;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCost;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;

/**
 * Relational operator that returns the sorted contents of a table and allow to lookup rows by specified bounds.
 */
public class IgniteSortedIndexSpool extends AbstractIgniteSpool implements IgniteRel {
    private static final String REL_TYPE_NAME = "SortedIndexSpool";

    private final RelCollation collation;

    /** Index search conditions. */
    private final List<SearchBounds> searchBounds;

    /** Filters. */
    protected final RexNode condition;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public IgniteSortedIndexSpool(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            RelCollation collation,
            RexNode condition,
            List<SearchBounds> searchBounds
    ) {
        super(cluster, traits, Type.LAZY, input);

        assert Objects.nonNull(searchBounds);
        assert Objects.nonNull(condition);

        this.searchBounds = searchBounds;
        this.condition = condition;
        this.collation = collation;
    }

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteSortedIndexSpool(RelInput input) {
        this(input.getCluster(),
                input.getTraitSet().replace(IgniteConvention.INSTANCE),
                input.getInputs().get(0),
                input.getCollation(),
                input.getExpression("condition"),
                ((RelInputEx) input).getSearchBounds("searchBounds")
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
        shuttle.apply(condition);

        return super.accept(shuttle);
    }

    /**
     * Clone.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteSortedIndexSpool(cluster, getTraitSet(), inputs.get(0), collation, condition, searchBounds);
    }

    /** {@inheritDoc} */
    @Override
    protected Spool copy(RelTraitSet traitSet, RelNode input, Type readType, Type writeType) {
        return new IgniteSortedIndexSpool(getCluster(), traitSet, input, collation, condition, searchBounds);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isEnforcer() {
        return true;
    }

    /**
     * ExplainTerms.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        RelWriter writer = super.explainTerms(pw);

        writer.item("condition", condition);
        writer.item("collation", collation);
        writer.itemIf("searchBounds", searchBounds, searchBounds != null);

        return writer;
    }

    /** {@inheritDoc} */
    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return mq.getRowCount(getInput()) * mq.getSelectivity(this, null);
    }

    /**
     * Get index search conditions.
     */
    public List<SearchBounds> searchBounds() {
        return searchBounds;
    }

    /**
     * Get collation.
     */
    @Override
    public RelCollation collation() {
        return collation;
    }

    /**
     * Get condition.
     */
    public RexNode condition() {
        return condition;
    }

    /** {@inheritDoc} */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCnt = mq.getRowCount(getInput());
        double bytesPerRow = getRowType().getFieldCount() * IgniteCost.AVERAGE_FIELD_SIZE;
        double totalBytes = rowCnt * bytesPerRow;
        double cpuCost;

        if (searchBounds != null) {
            cpuCost = Math.log(rowCnt) * IgniteCost.ROW_COMPARISON_COST;
        } else {
            cpuCost = rowCnt * IgniteCost.ROW_PASS_THROUGH_COST;
        }

        IgniteCostFactory costFactory = (IgniteCostFactory) planner.getCostFactory();

        return costFactory.makeCost(rowCnt, cpuCost, 0, totalBytes, 0);
    }

    /** {@inheritDoc} */
    @Override
    public String getRelTypeName() {
        return REL_TYPE_NAME;
    }
}
