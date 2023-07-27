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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.MapReduceAggregates;
import org.apache.ignite.internal.sql.engine.rel.agg.MapReduceAggregates.AggregateRelBuilder;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
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

        /** {@inheritDoc} */
        @Override
        @Nullable
        protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalAggregate agg) {
            if (HintUtils.isExpandDistinctAggregate(agg) || agg.getGroupSets().size() > 1) {
                return null;
            }

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

        /** {@inheritDoc} */
        @Override
        protected @Nullable PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalAggregate agg) {
            if ((nullOrEmpty(agg.getGroupSet()) && agg.getGroupSets().isEmpty()) || agg.getGroupSets().size() > 1) {
                return null;
            }

            if (complexDistinctAgg(agg.getAggCallList())
                    || !canBeImplementedAsMapReduce(agg.getAggCallList())
                    || HintUtils.isExpandDistinctAggregate(agg)) {
                return null;
            }

            RelOptCluster cluster = agg.getCluster();
            RelCollation collation = TraitUtils.createCollation(agg.getGroupSet().asList());

            RelTraitSet inTrait = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(collation);
            RelTraitSet outTrait = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(collation);

            AggregateRelBuilder relBuilder = new AggregateRelBuilder() {
                @Override
                public IgniteRel makeMapAgg(RelOptCluster cluster, RelNode input, ImmutableBitSet groupSet,
                        List<ImmutableBitSet> groupSets, List<AggregateCall> aggregateCalls) {

                    return new IgniteMapSortAggregate(
                            cluster,
                            outTrait.replace(IgniteDistributions.random()),
                            convert(input, inTrait.replace(IgniteDistributions.random())),
                            agg.getGroupSet(),
                            agg.getGroupSets(),
                            aggregateCalls,
                            collation
                    );
                }

                @Override
                public IgniteRel makeReduceAgg(RelOptCluster cluster, RelNode map, ImmutableBitSet groupSet,
                        List<ImmutableBitSet> groupSets, List<AggregateCall> aggregateCalls, RelDataType outputType) {

                    return new IgniteReduceSortAggregate(
                            cluster,
                            outTrait.replace(IgniteDistributions.single()),
                            convert(map, inTrait.replace(IgniteDistributions.single())),
                            agg.getGroupSet(),
                            agg.getGroupSets(),
                            aggregateCalls,
                            outputType,
                            collation
                    );
                }
            };

            return MapReduceAggregates.buildAggregates(agg, relBuilder);
        }
    }
}
