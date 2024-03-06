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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteRelVisitor;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * Physical node for MAP phase of MINUS (EXCEPT) operator.
 */
public class IgniteMapMinus extends IgniteMinus implements IgniteMapSetOp {
    private static final String REL_TYPE_NAME = "MapMinus";

    /**
     * Constructor.
     *
     * @param cluster Cluster that this relational expression belongs to.
     * @param traitSet  The traits of this rel.
     * @param inputs  Input relational expressions.
     * @param all Whether this operator should return all rows or only distinct rows.
     */
    public IgniteMapMinus(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelNode> inputs,
            boolean all
    ) {
        super(cluster, traitSet, inputs, all);
    }

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public IgniteMapMinus(RelInput input) {
        super(input);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteMapMinus copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new IgniteMapMinus(getCluster(), traitSet, inputs, all);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteMapMinus(cluster, getTraitSet(), Commons.cast(inputs), all);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    protected RelDataType deriveRowType() {
        IgniteTypeFactory typeFactory = (IgniteTypeFactory) getCluster().getTypeFactory();
        RelDataType rowType = getInput(0).getRowType();

        return IgniteMapSetOp.buildRowType(typeFactory, rowType, COUNTER_FIELDS_CNT);
    }

    /** {@inheritDoc} */
    @Override
    public int aggregateFieldsCount() {
        return getInput(0).getRowType().getFieldCount() + COUNTER_FIELDS_CNT;
    }

    /** {@inheritDoc} */
    @Override
    public String getRelTypeName() {
        return REL_TYPE_NAME;
    }
}
