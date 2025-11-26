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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.ignite.internal.sql.engine.rel.IgniteSort;
import org.immutables.value.Value;

/**
 * A rule that merges {@link IgniteSort} on top of another {@link IgniteSort} together.
 */
@Value.Enclosing
public class SortMergeRule extends RelRule<SortMergeRule.Config> {
    public static final RelOptRule INSTANCE = Config.INSTANCE.toRule();

    private SortMergeRule(Config cfg) {
        super(cfg);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        IgniteSort topSort = call.rel(0);
        IgniteSort bottomSort = call.rel(1);

        // Top sort has Limit semantic and bottom is just an enforcer.
        // In this case let's just prune bottom sort.
        if (bottomSort.isEnforcer() && !topSort.isEnforcer()) {
            topSort.getCluster().getPlanner().prune(bottomSort);

            return;
        }

        // Bottom sort has limit semantic, but top sort has similar yet more strict collation.
        // In this case we return Limit sort with strict collation 
        if (!bottomSort.isEnforcer() && topSort.isEnforcer() && topSort.collation.satisfies(bottomSort.collation)) {
            topSort.getCluster().getPlanner().prune(topSort);

            call.transformTo(
                    new IgniteSort(
                            topSort.getCluster(),
                            topSort.getTraitSet(),
                            bottomSort.getInput(),
                            topSort.collation,
                            bottomSort.offset,
                            bottomSort.fetch
                    )
            );
        }
    }

    /** Configuration. */
    @SuppressWarnings({"ClassNameSameAsAncestorName", "InnerClassFieldHidesOuterClassField"})
    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config INSTANCE = ImmutableSortMergeRule.Config.of()
                .withDescription("SortMergeRule")
                .withOperandSupplier(o0 ->
                        o0.operand(IgniteSort.class).oneInput(o1 ->
                                o1.operand(IgniteSort.class).anyInputs()))
                .as(Config.class);

        /** {@inheritDoc} */
        @Override
        default SortMergeRule toRule() {
            return new SortMergeRule(this);
        }
    }
}
