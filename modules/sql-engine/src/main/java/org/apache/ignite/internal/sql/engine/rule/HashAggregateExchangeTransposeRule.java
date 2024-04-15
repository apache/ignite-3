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
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.MapReduceAggregates;
import org.apache.ignite.internal.sql.engine.rel.agg.MapReduceAggregates.AggregateRelBuilder;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.HintUtils;
import org.immutables.value.Value;

/**
 * A rule that pushes {@link IgniteColocatedHashAggregate} node under {@link IgniteExchange} if possible, otherwise, splits into map-reduce
 * phases.
 */
@Value.Enclosing
public class HashAggregateExchangeTransposeRule extends RelRule<HashAggregateExchangeTransposeRule.Config> {
    public static final RelOptRule HASH_AGGREGATE_PUSH_DOWN = HashAggregateExchangeTransposeRule.Config.DEFAULT.toRule();

    HashAggregateExchangeTransposeRule(HashAggregateExchangeTransposeRule.Config cfg) {
        super(cfg);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        IgniteColocatedHashAggregate aggregate = call.rel(0);
        IgniteExchange exchange = call.rel(1);

        // Conditions already checked by IgniteColocatedHashAggregate
        assert !HintUtils.isExpandDistinctAggregate(aggregate);

        return exchange.distribution() == single()
                && hashAlike(distribution(exchange.getInput()))
                && (canBePushedDown(aggregate, exchange) || canBeConvertedToMapReduce(aggregate));
    }

    /**
     * Returns {@code true} if an aggregate groups by distribution columns and therefore, can be pushed down under Exchange as-is,
     * {@code false} otherwise.
     */
    private static boolean canBePushedDown(IgniteColocatedHashAggregate aggregate, IgniteExchange exchange) {
        return distribution(exchange.getInput()).getType() == Type.HASH_DISTRIBUTED
                && !nullOrEmpty(aggregate.getGroupSet())
                && ImmutableBitSet.of(distribution(exchange.getInput()).getKeys()).equals(aggregate.getGroupSet());
    }

    /**
     * Returns {@code true} if an aggregate can be converted to map-reduce phases, {@code false} otherwise.
     */
    private static boolean canBeConvertedToMapReduce(IgniteColocatedHashAggregate aggregate) {
        return canBeImplementedAsMapReduce(aggregate.getAggCallList())
                && !complexDistinctAgg(aggregate.getAggCallList());
    }

    private static boolean hashAlike(IgniteDistribution distribution) {
        return distribution.getType() == Type.HASH_DISTRIBUTED
                || distribution.getType() == Type.RANDOM_DISTRIBUTED;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        IgniteColocatedHashAggregate agg = call.rel(0);
        IgniteExchange exchange = call.rel(1);

        RelOptCluster cluster = agg.getCluster();

        if (canBePushedDown(agg, exchange)) {
            assert agg.getGroupSets().size() == 1;
            assert agg.collation().getKeys().isEmpty();

            RelTraitSet inTrait = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(distribution(exchange.getInput()));
            RelTraitSet outTrait =  agg.getTraitSet().replace(distribution(exchange.getInput()));

            cluster.getPlanner().prune(agg);

            IgniteExchange relNode = new IgniteExchange(
                    cluster,
                    agg.getTraitSet(),
                    new IgniteColocatedHashAggregate(
                            cluster,
                            outTrait,
                            convert(exchange.getInput(), inTrait),
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

        RelTraitSet inTrait = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelTraitSet outTrait = cluster.traitSetOf(IgniteConvention.INSTANCE);

        AggregateRelBuilder relBuilder = new AggregateRelBuilder() {
            @Override
            public IgniteRel makeMapAgg(RelOptCluster cluster, RelNode input, ImmutableBitSet groupSet,
                    List<ImmutableBitSet> groupSets, List<AggregateCall> aggregateCalls) {

                return new IgniteMapHashAggregate(
                        cluster,
                        outTrait.replace(distribution(input)),
                        // TODO: without conversion sibling rule SortedAggregateExchangeTransposeRules doesn't apply. why?
                        convert(input, inTrait.replace(distribution(input))),
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
                assert distribution(input) == single();

                return new IgniteReduceHashAggregate(
                        cluster,
                        outTrait.replace(single()),
                        input,
                        groupSet,
                        groupSets,
                        aggregateCalls,
                        outputType
                );
            }
        };

        call.transformTo(MapReduceAggregates.buildAggregates(agg, exchange, relBuilder, fieldMappingOnReduce));
    }

    /** Configuration. */
    @SuppressWarnings({"ClassNameSameAsAncestorName", "InnerClassFieldHidesOuterClassField"})
    @Value.Immutable
    public interface Config extends RelRule.Config {
        HashAggregateExchangeTransposeRule.Config DEFAULT = ImmutableHashAggregateExchangeTransposeRule.Config.of()
                .withDescription("HashAggregateExchangeTransposeRule")
                .withOperandSupplier(o0 ->
                        o0.operand(IgniteColocatedHashAggregate.class)
                                .oneInput(o1 ->
                                        o1.operand(IgniteExchange.class)
                                                .anyInputs()))
                .as(HashAggregateExchangeTransposeRule.Config.class);

        /** {@inheritDoc} */
        @Override
        default HashAggregateExchangeTransposeRule toRule() {
            return new HashAggregateExchangeTransposeRule(this);
        }
    }
}
