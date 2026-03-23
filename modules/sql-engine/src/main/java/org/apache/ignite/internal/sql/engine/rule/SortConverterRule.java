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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteLimit;
import org.apache.ignite.internal.sql.engine.rel.IgniteSort;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.immutables.value.Value;

/**
 * Converter rule for sort operator.
 */
@Value.Enclosing
public class SortConverterRule extends RelRule<SortConverterRule.Config> {
    public static final RelOptRule INSTANCE = SortConverterRule.Config.DEFAULT.toRule();

    /** Creates a LimitConverterRule. */
    protected SortConverterRule(SortConverterRule.Config config) {
        super(config);
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        SortConverterRule.Config DEFAULT = ImmutableSortConverterRule.Config.of()
                .withOperandSupplier(b ->
                        b.operand(LogicalSort.class).anyInputs());

        /** {@inheritDoc} */
        @Override
        default SortConverterRule toRule() {
            return new SortConverterRule(this);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onMatch(RelOptRuleCall call) {
        Sort sort = call.rel(0);
        RelOptCluster cluster = sort.getCluster();

        RelTraitSet traits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                .replace(sort.getCollation())
                .replace(IgniteDistributions.single());

        // offset-only case is not supported by SortNode, therefore we need to create
        // plain Sort and Limit node with offset on top.
        if (sort.collation == RelCollations.EMPTY || (sort.fetch == null && sort.offset != null)) {
            call.transformTo(new IgniteLimit(
                    cluster, traits, convert(sort.getInput(), traits), sort.offset, sort.fetch
            ));

            return;
        }

        call.transformTo(new IgniteSort(
                cluster, traits, convert(sort.getInput(), traits.replace(RelCollations.EMPTY)),
                sort.getCollation(), sort.offset, sort.fetch
        ));
    }
}
