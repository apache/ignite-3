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

package org.apache.ignite.internal.sql.engine.rel.agg;

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
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteRelVisitor;
import org.apache.ignite.internal.sql.engine.rel.explain.IgniteRelWriter;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.PlanUtils;

/**
 * IgniteMapSortAggregate.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class IgniteMapSortAggregate extends IgniteMapAggregateBase implements IgniteSortAggregateBase {
    private static final String REL_TYPE_NAME = "MapSortAggregate";

    /** Collation. */
    private final RelCollation collation;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public IgniteMapSortAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            RelCollation collation
    ) {
        super(cluster, traitSet, input, groupSet, groupSets, aggCalls);

        assert Objects.nonNull(collation);

        this.collation = collation;
    }

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteMapSortAggregate(RelInput input) {
        super(TraitUtils.changeTraits(input, IgniteConvention.INSTANCE));

        collation = input.getCollation();

        assert Objects.nonNull(collation);
    }

    /** {@inheritDoc} */
    @Override
    public Aggregate copy(
            RelTraitSet traitSet,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        return new IgniteMapSortAggregate(
                getCluster(), traitSet, input, groupSet, groupSets, aggCalls, this.collation);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteMapSortAggregate(
                cluster,
                getTraitSet().replace(collation),
                sole(inputs),
                getGroupSet(),
                getGroupSets(),
                getAggCallList(),
                collation
        );
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("collation", collation);
    }

    /** {@inheritDoc} */
    @Override
    protected RelDataType deriveRowType() {
        IgniteTypeFactory typeFactory = Commons.typeFactory(getCluster());

        return PlanUtils.createSortAggRowType(groupSet, typeFactory, input.getRowType(), aggCalls);
    }

    /** {@inheritDoc} */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return computeSelfCostSort(planner, mq);
    }

    /** {@inheritDoc} */
    @Override
    public RelCollation collation() {
        return collation;
    }

    /** {@inheritDoc} */
    @Override public String getRelTypeName() {
        return REL_TYPE_NAME;
    }

    @Override
    public IgniteRelWriter explain(IgniteRelWriter writer) {
        return super.explain(writer).addCollation(collation, getRowType());
    }
}
