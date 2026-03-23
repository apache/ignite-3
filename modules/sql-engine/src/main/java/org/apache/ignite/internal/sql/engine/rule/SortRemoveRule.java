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
import org.apache.ignite.internal.sql.engine.rel.IgniteLimit;
import org.apache.ignite.internal.sql.engine.rel.IgniteSort;
import org.immutables.value.Value;

/**
 * A rule that removes {@link IgniteSort}  if its input is already sorted.
 */
@Value.Enclosing
public class SortRemoveRule extends RelRule<SortRemoveRule.Config> {
    public static final RelOptRule INSTANCE = Config.INSTANCE.toRule();

    private SortRemoveRule(Config cfg) {
        super(cfg);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        IgniteSort sort = call.rel(0);

        if (sort.offset == null && sort.fetch == null) {
            // Simple case, just remove the sort
            call.transformTo(convert(sort.getInput(), sort.getTraitSet()));

            return;
        }

        // Otherwise replace with Limit node.
        call.transformTo(new IgniteLimit(
                sort.getCluster(),
                sort.getTraitSet(),
                convert(sort.getInput(), sort.getTraitSet()),
                sort.offset,
                sort.fetch
        ));
    }

    /** Configuration. */
    @SuppressWarnings({"ClassNameSameAsAncestorName", "InnerClassFieldHidesOuterClassField"})
    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config INSTANCE = ImmutableSortRemoveRule.Config.of()
                .withDescription("SortRemoveRule")
                .withOperandSupplier(o0 ->
                        o0.operand(IgniteSort.class)
                                .anyInputs())
                .as(Config.class);

        /** {@inheritDoc} */
        @Override
        default SortRemoveRule toRule() {
            return new SortRemoveRule(this);
        }
    }
}
