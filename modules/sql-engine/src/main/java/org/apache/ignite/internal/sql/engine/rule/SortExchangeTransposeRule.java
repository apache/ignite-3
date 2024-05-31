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

import static org.apache.ignite.internal.sql.engine.trait.IgniteDistributions.single;
import static org.apache.ignite.internal.sql.engine.trait.TraitUtils.distribution;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution.Type;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteSort;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.immutables.value.Value;

/**
 * A rule that pushes {@link IgniteSort} node under {@link IgniteExchange}.
 */
@Value.Enclosing
public class SortExchangeTransposeRule extends RelRule<SortExchangeTransposeRule.Config> {
    public static final RelOptRule INSTANCE = Config.INSTANCE.toRule();

    private SortExchangeTransposeRule(Config cfg) {
        super(cfg);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        IgniteExchange exchange = call.rel(1);

        return hashAlike(distribution(exchange.getInput()))
                && exchange.distribution() == single() && call.rel(0).isEnforcer();
    }

    private static boolean hashAlike(IgniteDistribution distribution) {
        return distribution.getType() == Type.HASH_DISTRIBUTED
                || distribution.getType() == Type.RANDOM_DISTRIBUTED;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        IgniteSort sort = call.rel(0);

        assert sort.isEnforcer() : "Non enforcer sort can not be pushed under Exchanger";

        IgniteExchange exchange = call.rel(1);

        RelOptCluster cluster = sort.getCluster();
        RelCollation collation = sort.collation();
        RelNode input = exchange.getInput();

        call.transformTo(
                new IgniteExchange(
                        cluster,
                        exchange.getTraitSet()
                                .replace(collation),
                        convert(input, input.getTraitSet().replace(collation)),
                        exchange.distribution()
                )
        );
    }

    /** Configuration. */
    @SuppressWarnings({"ClassNameSameAsAncestorName", "InnerClassFieldHidesOuterClassField"})
    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config INSTANCE = ImmutableSortExchangeTransposeRule.Config.of()
                .withDescription("SortExchangeTransposeRule")
                .withOperandSupplier(o0 ->
                        o0.operand(IgniteSort.class)
                                // Only an enforcer sort (a sort operator w/o fetch/offset parameters) can be pushed under exchange
                                .predicate(IgniteSort::isEnforcer)
                                .oneInput(o1 ->
                                        o1.operand(IgniteExchange.class)
                                                .anyInputs()))
                .as(Config.class);

        /** {@inheritDoc} */
        @Override
        default SortExchangeTransposeRule toRule() {
            return new SortExchangeTransposeRule(this);
        }
    }
}
