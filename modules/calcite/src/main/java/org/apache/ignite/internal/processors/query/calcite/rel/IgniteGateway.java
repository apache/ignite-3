/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.rel;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.extension.MyConvention;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;

import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;

/**
 *
 */
public class IgniteGateway extends SingleRel implements SourceAwareIgniteRel {
    private final long sourceId;

    /** */
    public IgniteGateway(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        this(-1L, cluster, traits, input);
    }

    /** */
    public IgniteGateway(long sourceId, RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);

        this.sourceId = sourceId;
    }

    public IgniteGateway(RelInput input) {
        super(input.getCluster(), input.getTraitSet().replace(IgniteConvention.INSTANCE), input.getInput());

        Object srcIdObj = input.get("sourceId");
        if (srcIdObj != null)
            sourceId = ((Number)srcIdObj).longValue();
        else
            sourceId = -1;
    }

    /** {@inheritDoc} */
    @Override public boolean isEnforcer() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(getInput());

        return planner.getCostFactory().makeCost(rowCount,
            rowCount * (IgniteCost.ROW_COMPARISON_COST + IgniteCost.ROW_PASS_THROUGH_COST), 0);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteGateway(cluster, getTraitSet(), sole(inputs));
    }

    /** {@inheritDoc} */
    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IgniteGateway(getCluster(), traitSet, sole(inputs));
    }

    /** {@inheritDoc} */
    @Override public long sourceId() {
        return sourceId;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(long sourceId) {
        return new IgniteGateway(sourceId, getCluster(), getTraitSet(), getInput());
    }
}
