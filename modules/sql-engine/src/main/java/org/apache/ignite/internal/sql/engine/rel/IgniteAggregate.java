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

import static org.apache.ignite.internal.sql.engine.trait.TraitUtils.changeTraits;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCost;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.sql.engine.rel.explain.IgniteRelWriter;

/**
 * IgniteAggregate.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public abstract class IgniteAggregate extends Aggregate implements IgniteRel {
    /** {@inheritDoc} */
    protected IgniteAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls
    ) {
        super(cluster, traitSet, List.of(), input, groupSet, groupSets, aggCalls);
    }

    /** {@inheritDoc} */
    protected IgniteAggregate(RelInput input) {
        super(changeTraits(input, IgniteConvention.INSTANCE));
    }

    /** {@inheritDoc} */
    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        Double groupsCnt = mq.getDistinctRowCount(getInput(), groupSet, null);

        if (groupsCnt != null) {
            return groupsCnt;
        }

        return guessDistinctRows(mq, groupSet.cardinality());
    }

    /**
     * EstimateMemoryForGroup.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public double estimateMemoryForGroup(RelMetadataQuery mq) {
        double mem = groupSet.cardinality() * IgniteCost.AVERAGE_FIELD_SIZE;

        if (!aggCalls.isEmpty()) {
            double grps = estimateRowCount(mq);

            for (AggregateCall aggCall : aggCalls) {
                if (aggCall.isDistinct()) {
                    ImmutableBitSet aggGroup = ImmutableBitSet.of(aggCall.getArgList());

                    double distinctRows = guessDistinctRows(mq, aggGroup.cardinality());

                    mem += IgniteCost.AGG_CALL_MEM_COST * distinctRows / grps;
                } else {
                    mem += IgniteCost.AGG_CALL_MEM_COST;
                }
            }
        }

        return mem;
    }

    /** Implements heuristics estimation for distinct row count. */
    private double guessDistinctRows(RelMetadataQuery mq, int groupSize) {
        if (groupSize == 0) {
            return 1;
        } else {
            double rowCount = mq.getRowCount(getInput());
            rowCount *= (1.0 - Math.pow(.8, groupSize));
            return rowCount;
        }
    }

    /**
     * ComputeSelfCostHash.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public RelOptCost computeSelfCostHash(RelOptPlanner planner, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory) planner.getCostFactory();

        double inRows = mq.getRowCount(getInput());
        double groups = estimateRowCount(mq);

        return costFactory.makeCost(
                inRows,
                inRows * IgniteCost.ROW_PASS_THROUGH_COST,
                0,
                groups * estimateMemoryForGroup(mq),
                0
        );
    }

    /**
     * ComputeSelfCostSort.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public RelOptCost computeSelfCostSort(RelOptPlanner planner, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory) planner.getCostFactory();

        double inRows = mq.getRowCount(getInput());

        return costFactory.makeCost(
                inRows,
                inRows * IgniteCost.ROW_PASS_THROUGH_COST,
                0,
                estimateMemoryForGroup(mq),
                0
        );
    }

    @Override
    public IgniteRelWriter explain(IgniteRelWriter writer) {
        RelDataType rowType = getInput().getRowType();

        writer
                .addGroup(groupSet, rowType)
                .addAggregation(aggCalls, rowType);

        if (getGroupType() != Group.SIMPLE) {
            writer.addGroupSets(groupSets, rowType);
        }

        return writer;
    }
}
