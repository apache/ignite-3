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
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollation;
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
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.MapReduceAggregates;
import org.apache.ignite.internal.sql.engine.rel.agg.MapReduceAggregates.AggregateRelBuilder;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.HintUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Planner rule that recognizes a {@link org.apache.calcite.rel.core.Aggregate}
 * and in relation to distribution and additional conditions produce appropriate node, require sorted input.
 */
public class SortAggregateConverterRule {
    public static final RelOptRule COLOCATED = new ColocatedSortAggregateConverterRule();

    public static final RelOptRule MAP_REDUCE = new MapReduceSortAggregateConverterRule();

    private SortAggregateConverterRule() {
        // No-op.
    }

    private static class ColocatedSortAggregateConverterRule extends AbstractIgniteConverterRule<LogicalAggregate> {
        ColocatedSortAggregateConverterRule() {
            super(LogicalAggregate.class, "ColocatedSortAggregateConverterRule");
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            LogicalAggregate aggregate = call.rel(0);

            return !HintUtils.isExpandDistinctAggregate(aggregate)
                    && aggregate.getGroupSets().size() == 1;
        }

        /** {@inheritDoc} */
        @Override
        @Nullable
        protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalAggregate agg) {
            RelCollation collation = TraitUtils.createCollation(agg.getGroupSet().asList());

            RelOptCluster cluster = agg.getCluster();

            RelTraitSet inTrait = cluster.traitSetOf(IgniteConvention.INSTANCE)
                    .replace(collation)
                    .replace(IgniteDistributions.single());

            RelTraitSet outTrait = cluster.traitSetOf(IgniteConvention.INSTANCE)
                    .replace(collation)
                    .replace(IgniteDistributions.single());

            RelNode input = convert(agg.getInput(), inTrait);

            return new IgniteColocatedSortAggregate(
                    cluster,
                    outTrait,
                    input,
                    agg.getGroupSet(),
                    agg.getGroupSets(),
                    agg.getAggCallList()
            );
        }
    }

    private static class MapReduceSortAggregateConverterRule extends AbstractIgniteConverterRule<LogicalAggregate> {
        MapReduceSortAggregateConverterRule() {
            super(LogicalAggregate.class, "MapReduceSortAggregateConverterRule");
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            LogicalAggregate aggregate = call.rel(0);

            return !HintUtils.isExpandDistinctAggregate(aggregate)
                    && (nullOrEmpty(aggregate.getGroupSet()) || aggregate.getGroupSets().size() == 1)
                    && canBeImplementedAsMapReduce(aggregate.getAggCallList())
                    && !complexDistinctAgg(aggregate.getAggCallList());
        }

        /** {@inheritDoc} */
        @Override
        protected @Nullable PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalAggregate agg) {
            RelOptCluster cluster = agg.getCluster();
            RelCollation collation = TraitUtils.createCollation(agg.getGroupSet().asList());

            // Create mapping to adjust fields on REDUCE phase.
            Mapping fieldMappingOnReduce = Commons.trimmingMapping(agg.getGroupSet().length(), agg.getGroupSet());

            // Adjust columns in output collation.
            RelCollation outputCollation = collation.apply(fieldMappingOnReduce);

            RelTraitSet inTraits = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(collation);
            RelTraitSet outTraits = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(outputCollation);

            AggregateRelBuilder relBuilder = new AggregateRelBuilder() {

                @Override
                public IgniteRel makeMapAgg(RelOptCluster cluster, RelNode input, ImmutableBitSet groupSet,
                        List<ImmutableBitSet> groupSets, List<AggregateCall> aggregateCalls) {

                    return new IgniteMapSortAggregate(
                            cluster,
                            outTraits.replace(IgniteDistributions.random()),
                            convert(input, inTraits.replace(IgniteDistributions.random())),
                            groupSet,
                            groupSets,
                            aggregateCalls,
                            collation
                    );
                }

                @Override
                public IgniteRel makeProject(RelOptCluster cluster, RelNode input, List<RexNode> reduceInputExprs,
                        RelDataType projectRowType) {

                    return new IgniteProject(agg.getCluster(),
                            outTraits.replace(IgniteDistributions.single()),
                            convert(input, outTraits.replace(IgniteDistributions.single())),
                            reduceInputExprs,
                            projectRowType
                    );
                }

                @Override
                public IgniteRel makeReduceAgg(RelOptCluster cluster, RelNode input, ImmutableBitSet groupSet,
                        List<ImmutableBitSet> groupSets, List<AggregateCall> aggregateCalls, RelDataType outputType) {

                    return new IgniteReduceSortAggregate(
                            cluster,
                            outTraits.replace(IgniteDistributions.single()),
                            convert(input, outTraits.replace(IgniteDistributions.single())),
                            groupSet,
                            groupSets,
                            aggregateCalls,
                            outputType,
                            outputCollation
                    );
                }
            };

            return MapReduceAggregates.buildAggregates(agg, relBuilder, fieldMappingOnReduce);
        }
    }
}
