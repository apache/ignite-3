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
import static org.apache.ignite.internal.sql.engine.trait.IgniteDistributions.single;
import static org.apache.ignite.internal.sql.engine.trait.TraitUtils.distribution;
import static org.apache.ignite.internal.sql.engine.util.PlanUtils.complexDistinctAgg;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution.Type;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.MapReduceAggregates;
import org.apache.ignite.internal.sql.engine.rel.agg.MapReduceAggregates.AggregateRelBuilder;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.HintUtils;
import org.immutables.value.Value;

/**
 * A rule that pushes {@link IgniteColocatedSortAggregate} node under {@link IgniteExchange} if possible, otherwise, splits into map-reduce
 * phases.
 */
@Value.Enclosing
public class SortAggregatePushDownRule extends RelRule<SortAggregatePushDownRule.Config> {
    public static final RelOptRule SORT_AGGREGATE_PUSH_DOWN = SortAggregatePushDownRule.Config.DEFAULT.toRule();

    private SortAggregatePushDownRule(SortAggregatePushDownRule.Config cfg) {
        super(cfg);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        IgniteColocatedSortAggregate aggregate = call.rel(0);
        IgniteExchange exchange = call.rel(1);

        // Conditions already checked by SortAggregateConverterRule
        assert !HintUtils.isExpandDistinctAggregate(aggregate);
        assert aggregate.getGroupSets().size() == 1;

        return hashAlike(distribution(exchange.getInput()))
                && exchange.distribution() == single()
                && (canBePushedDown(aggregate, exchange) || canBeConvertedToMapReduce(aggregate));
    }

    /**
     * Returns {@code true} if an aggregate groups by distribution columns and therefore, can be pushed down under Exchange as-is,
     * {@code false} otherwise.
     */
    private static boolean canBePushedDown(IgniteColocatedSortAggregate aggregate, IgniteExchange exchange) {
        return distribution(exchange.getInput()).getType() == Type.HASH_DISTRIBUTED
                && !nullOrEmpty(aggregate.getGroupSet())
                && ImmutableBitSet.of(distribution(exchange.getInput()).getKeys()).equals(aggregate.getGroupSet());
    }

    /**
     * Returns {@code true} if an aggregate can be converted to map-reduce phases, {@code false} otherwise.
     */
    private static boolean canBeConvertedToMapReduce(IgniteColocatedSortAggregate aggregate) {
        return canBeImplementedAsMapReduce(aggregate.getAggCallList())
                && !complexDistinctAgg(aggregate.getAggCallList());
    }

    private static boolean hashAlike(IgniteDistribution distribution) {
        return distribution.getType() == Type.HASH_DISTRIBUTED
                || distribution.getType() == Type.RANDOM_DISTRIBUTED;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        IgniteColocatedSortAggregate agg = call.rel(0);
        IgniteExchange exchange = call.rel(1);

        RelOptCluster cluster = agg.getCluster();
        RelCollation collation = agg.collation();

        if (canBePushedDown(agg, exchange)) {
            assert agg.getGroupSets().size() == 1;
            assert !agg.collation().getKeys().isEmpty();

            RelNode input = exchange.getInput();

            RelTraitSet inTrait = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(collation).replace(distribution(input));
            RelTraitSet outTrait =  agg.getTraitSet().replace(collation).replace(distribution(input));

            cluster.getPlanner().prune(agg);

            IgniteExchange relNode = new IgniteExchange(
                    cluster,
                    exchange.getTraitSet().replace(collation),
                    new IgniteColocatedSortAggregate(
                            cluster,
                            outTrait,
                            convert(input, inTrait),
                            agg.getGroupSet(),
                            agg.getGroupSets(),
                            agg.getAggCallList()
                    ),
                    exchange.distribution()
            );

            call.transformTo(relNode);

            return;
        }

        // Create mapping to adjust fields on REDUCE phase.
        Mapping fieldMappingOnReduce = Commons.trimmingMapping(agg.getGroupSet().length(), agg.getGroupSet());

        // Adjust columns in output collation.
        RelCollation outputCollation = collation.apply(fieldMappingOnReduce);

        RelTraitSet inTrait = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(collation).replace(distribution(exchange.getInput()));
        RelTraitSet outTraits = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(outputCollation);

        AggregateRelBuilder relBuilder = new AggregateRelBuilder() {
            @Override
            public IgniteRel makeMapAgg(RelOptCluster cluster, RelNode input, ImmutableBitSet groupSet,
                    List<ImmutableBitSet> groupSets, List<AggregateCall> aggregateCalls) {

                return new IgniteMapSortAggregate(
                        cluster,
                        outTraits.replace(distribution(input)),
                        convert(input, inTrait),
                        groupSet,
                        groupSets,
                        aggregateCalls,
                        collation
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
                assert distribution(input) == single();

                return new IgniteReduceSortAggregate(
                        cluster,
                        outTraits.replace(single()),
                        input,
                        groupSet,
                        groupSets,
                        aggregateCalls,
                        outputType,
                        collation
                );
            }
        };

        call.transformTo(MapReduceAggregates.buildAggregates(agg, exchange, relBuilder, fieldMappingOnReduce));
    }

    /** Configuration. */
    @SuppressWarnings({"ClassNameSameAsAncestorName"})
    @Value.Immutable
    public interface Config extends RelRule.Config {
        SortAggregatePushDownRule.Config DEFAULT = ImmutableSortAggregatePushDownRule.Config.builder()
                .withDescription("MapReduceSortAggregateConverterRule")
                .withOperandSupplier(o0 ->
                        o0.operand(IgniteColocatedSortAggregate.class)
                                .oneInput(o1 ->
                                        o1.operand(IgniteExchange.class)
                                                .anyInputs()))
                .build();

        /** {@inheritDoc} */
        @Override
        default SortAggregatePushDownRule toRule() {
            return new SortAggregatePushDownRule(this);
        }
    }
}
