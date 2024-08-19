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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingService;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Relational operator that represents a {@code SELECT COUNT(*) FROM table} queries such as:
 *<ul>
 *     <li>{@code SELECT COUNT(*) FROM table}</li>
 *     <li>{@code SELECT COUNT(not_null_col) FROM table}</li>
 *     <li>{@code SELECT COUNT(1) FROM table}</li>
 *</ul>
 *
 * <p>If a select list includes constant literals or multiple {@code COUNT} calls
 * (e.g. {@code SELECT COUNT(*), 1, 2 FROM table} or  {@code SELECT COUNT(*), COUNT(1) FROM table} )
 * then such query can be presented by this operator as well.
 *
 * <p>Note: This operator can not be executed in transactional context and does not participate in distributed query plan.
 * Given that node is not supposed to be a part of distributed query plan, the following parts were
 * deliberately omitted:<ul>
 *     <li>this class doesn't implement {@link SourceAwareIgniteRel}, making it impossible
 *     to map properly by {@link MappingService}</li>
 *     <li>de-serialisation constructor is omitted (see {@link ProjectableFilterableTableScan#ProjectableFilterableTableScan(RelInput)}
 *     as example)</li>
 * </ul>
 */
public class IgniteSelectCount extends AbstractRelNode implements IgniteRel {

    private static final String REL_TYPE_NAME = "SelectCount";

    private final RelOptTable table;

    private final List<RexNode> expressions;

    /**
     * Constructor.
     *
     * @param cluster A cluster this relation belongs to.
     * @param traits A set of traits this node satisfies.
     * @param table A target table.
     * @param expressions Returns a list of expressions returned alongside COUNT(*).
     */
    public IgniteSelectCount(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelOptTable table,
            List<RexNode> expressions
    ) {
        super(cluster, traits);

        this.table = table;
        this.expressions = expressions;
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType deriveRowType() {
        return RexUtil.createStructType(Commons.typeFactory(getCluster()), expressions);
    }

    /** {@inheritDoc} */
    @Override
    public RelOptTable getTable() {
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

        return new IgniteSelectCount(cluster, getTraitSet(), table, expressions);
    }

    /** {@inheritDoc} */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("table", table.getQualifiedName())
                .itemIf("expressions", expressions, expressions.size() > 1);
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
