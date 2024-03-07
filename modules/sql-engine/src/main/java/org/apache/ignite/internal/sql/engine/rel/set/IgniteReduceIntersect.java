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

package org.apache.ignite.internal.sql.engine.rel.set;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteRelVisitor;

/**
 * Physical node for REDUCE phase of INTERSECT operator.
 */
public class IgniteReduceIntersect extends IgniteIntersect implements IgniteReduceSetOp {
    private static final String REL_TYPE_NAME = "ReduceIntersect";

    /**
     * Constructor.
     *
     * @param cluster Cluster that this relational expression belongs to.
     * @param traitSet The traits of this rel.
     * @param input Input relational expression.
     * @param all Whether this operator should return all rows or only distinct rows.
     * @param rowType Row type this expression produces.
     */
    public IgniteReduceIntersect(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            boolean all,
            RelDataType rowType
    ) {
        super(cluster, traitSet, List.of(input), all);

        // Since REDUCE.inputRowType. != REDUCE.outputRowType,
        // we do not want for a call to SetOp::deriveRowType to take place,
        // because it is going to produce incorrect result.
        this.rowType = rowType;
    }

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteReduceIntersect(RelInput input) {
        this(
                input.getCluster(),
                input.getTraitSet().replace(IgniteConvention.INSTANCE),
                input.getInput(),
                input.getBoolean("all", false),
                input.getRowType("rowType")
        );
    }

    /** {@inheritDoc} */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw)
                .itemIf("rowType", rowType, pw.getDetailLevel() == SqlExplainLevel.ALL_ATTRIBUTES);

        return pw;
    }

    /** {@inheritDoc} */
    @Override
    public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new IgniteReduceIntersect(getCluster(), traitSet, sole(inputs), all, rowType);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteReduceIntersect clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteReduceIntersect(cluster, getTraitSet(), sole(inputs), all, rowType);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public int aggregateFieldsCount() {
        return getInput(0).getRowType().getFieldCount();
    }

    /** {@inheritDoc} */
    @Override
    public String getRelTypeName() {
        return REL_TYPE_NAME;
    }
}
