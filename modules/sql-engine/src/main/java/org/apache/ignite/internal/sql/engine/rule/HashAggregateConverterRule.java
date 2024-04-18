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

package org.apache.ignite.internal.sql.engine.rule;

import static org.apache.ignite.internal.sql.engine.rel.agg.MapReduceAggregates.canBeImplementedAsMapReduce;
import static org.apache.ignite.internal.sql.engine.util.PlanUtils.complexDistinctAgg;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.MapReduceAggregates;
import org.apache.ignite.internal.sql.engine.rel.agg.MapReduceAggregates.AggregateRelBuilder;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.HintUtils;

/**
 * Planner rule that recognizes a {@link org.apache.calcite.rel.core.Aggregate}
 * and in relation to distribution and additional conditions produce appropriate node.
 */
public class HashAggregateConverterRule {
    public static final RelOptRule COLOCATED = new ColocatedHashAggregateConverterRule();

    public static final RelOptRule MAP_REDUCE = new MapReduceHashAggregateConverterRule();

    private HashAggregateConverterRule() {
        // No-op.
    }

    private static class ColocatedHashAggregateConverterRule extends AbstractIgniteConverterRule<LogicalAggregate> {
        ColocatedHashAggregateConverterRule() {
            super(LogicalAggregate.class, "ColocatedHashAggregateConverterRule");
        }

        /** {@inheritDoc} */
        @Override
        protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq,
                LogicalAggregate agg) {
            if (HintUtils.isExpandDistinctAggregate(agg)) {
                return null;
            }

            RelOptCluster cluster = agg.getCluster();
            RelTraitSet inTrait = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(IgniteDistributions.single());
            RelTraitSet outTrait = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(IgniteDistributions.single());
            RelNode input = convert(agg.getInput(), inTrait);

            return new IgniteColocatedHashAggregate(
                    cluster,
                    outTrait,
                    input,
                    agg.getGroupSet(),
                    agg.getGroupSets(),
                    agg.getAggCallList()
            );
        }
    }

    private static class MapReduceHashAggregateConverterRule extends AbstractIgniteConverterRule<LogicalAggregate> {
        MapReduceHashAggregateConverterRule() {
            super(LogicalAggregate.class, "MapReduceHashAggregateConverterRule");
        }

        /** {@inheritDoc} */
        @Override
        protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq,
                LogicalAggregate agg) {
            if (complexDistinctAgg(agg.getAggCallList())
                    || !canBeImplementedAsMapReduce(agg.getAggCallList())
                    || HintUtils.isExpandDistinctAggregate(agg)) {
                return null;
            }

            RelOptCluster cluster = agg.getCluster();
            RelTraitSet inTrait = cluster.traitSetOf(IgniteConvention.INSTANCE);
            RelTraitSet outTrait = cluster.traitSetOf(IgniteConvention.INSTANCE);
            Mapping fieldMappingOnReduce = Commons.trimmingMapping(agg.getGroupSet().length(), agg.getGroupSet());

            RelTraitSet reducePhaseTraits = outTrait.replace(IgniteDistributions.single());

            AggregateRelBuilder relBuilder = new AggregateRelBuilder() {

                @Override
                public IgniteRel makeMapAgg(RelOptCluster cluster, RelNode input, ImmutableBitSet groupSet,
                        List<ImmutableBitSet> groupSets, List<AggregateCall> aggregateCalls) {
                    return new IgniteMapHashAggregate(
                            cluster,
                            outTrait.replace(IgniteDistributions.random()),
                            input,
                            groupSet,
                            groupSets,
                            aggregateCalls
                    );
                }

                @Override
                public IgniteRel makeProject(RelOptCluster cluster, RelNode input, List<RexNode> reduceInputExprs,
                        RelDataType projectRowType) {

                    return new IgniteProject(
                            agg.getCluster(),
                            input.getTraitSet(),
                            input,
                            reduceInputExprs,
                            projectRowType
                    );
                }

                @Override
                public IgniteRel makeReduceAgg(RelOptCluster cluster, RelNode input, ImmutableBitSet groupSet,
                        List<ImmutableBitSet> groupSets, List<AggregateCall> aggregateCalls, RelDataType outputType) {

                    return new IgniteReduceHashAggregate(
                            cluster,
                            reducePhaseTraits,
                            convert(input, inTrait.replace(IgniteDistributions.single())),
                            groupSet,
                            groupSets,
                            aggregateCalls,
                            outputType
                    );
                }
            };

            return MapReduceAggregates.buildAggregates(agg, relBuilder, fieldMappingOnReduce);
        }
    }
}
