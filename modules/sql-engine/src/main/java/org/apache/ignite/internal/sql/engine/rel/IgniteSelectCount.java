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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCostFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Relational operator that represents a SELECT COUNT(*) query.
 */
public class IgniteSelectCount extends AbstractRelNode implements IgniteRel {

    private static final String REL_TYPE_NAME = "SelectCount";

    private final RelOptTable table;
    private final List<RexNode> expressions;
    private final RelDataType rowType;

    /**
     * Constructor.
     *
     * @param cluster A cluster this relation belongs to.
     * @param traits A set of traits this node satisfies.
     * @param table A target table.
     * @param rowType A row type.
     * @param expressions Returns a list of expressions returned alongside COUNT(*).
     */
    public IgniteSelectCount(
            RelOptCluster cluster,
            RelTraitSet traits,
            @Nullable RelOptTable table,
            RelDataType rowType,
            List<RexNode> expressions
    ) {
        super(cluster, traits);

        assert rowType.getFieldCount() == expressions.size() :
                format("The number of expressions and columns in output type do not match:\n{}\n{}", expressions, rowType);

        this.table = table;
        this.rowType = rowType;
        this.expressions = expressions;
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType deriveRowType() {
        return rowType;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable RelOptTable getTable() {
        return table;
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        assert inputs.isEmpty() : inputs;

        return new IgniteSelectCount(cluster, getTraitSet(), table, rowType, expressions);
    }

    /** {@inheritDoc} */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        RelWriter relWriter = super.explainTerms(pw);
        if (table != null) {
            relWriter.item("table", table.getQualifiedName());
        }
        return relWriter.itemIf("expressions", expressions, expressions.size() > 1);
    }

    /** {@inheritDoc} */
    @Override
    public String getRelTypeName() {
        return REL_TYPE_NAME;
    }

    /**
     * Returns a list of expressions returned alongside COUNT(*).
     *
     * @return List of expressions returned alongside COUNT(*).
     */
    public List<RexNode> expressions() {
        return expressions;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory) planner.getCostFactory();

        return costFactory.makeTinyCost();
    }
}
