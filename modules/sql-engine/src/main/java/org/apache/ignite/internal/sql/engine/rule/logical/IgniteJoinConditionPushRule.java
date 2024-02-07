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

package org.apache.ignite.internal.sql.engine.rule.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rex.RexUtil;
import org.apache.ignite.internal.sql.engine.rule.logical.IgniteJoinConditionPushRule.IgniteJoinConditionPushRuleConfig;
import org.immutables.value.Value;

/**
 * Rule that pushes parts of the join condition to its inputs.
 *
 * <p>The only difference between this rule and {@link JoinConditionPushRule} is that
 * former converts join condition to conjunctive normal form (CNF).
 */
@Value.Enclosing
public class IgniteJoinConditionPushRule extends FilterJoinRule<IgniteJoinConditionPushRuleConfig> {
    public static final RelOptRule INSTANCE = IgniteJoinConditionPushRuleConfig.DEFAULT.toRule();

    /** Creates a JoinConditionPushRule. */
    private IgniteJoinConditionPushRule(IgniteJoinConditionPushRuleConfig config) {
        super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
        Join join = call.rel(0);

        join = join.copy(
                join.getTraitSet(),
                RexUtil.toCnf(join.getCluster().getRexBuilder(), join.getCondition()),
                join.getLeft(),
                join.getRight(),
                join.getJoinType(),
                join.isSemiJoinDone()
        );

        perform(call, null, join);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface IgniteJoinConditionPushRuleConfig extends FilterJoinRule.Config {
        IgniteJoinConditionPushRuleConfig DEFAULT = ImmutableIgniteJoinConditionPushRule.IgniteJoinConditionPushRuleConfig
                .of((join, joinType, exp) -> true)
                .withOperandSupplier(b ->
                        b.operand(Join.class).anyInputs())
                .withSmart(true);

        @Override default IgniteJoinConditionPushRule toRule() {
            return new IgniteJoinConditionPushRule(this);
        }
    }
}
