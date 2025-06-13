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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteLimit;
import org.apache.ignite.internal.sql.engine.rel.IgniteSort;
import org.apache.ignite.internal.sql.engine.sql.fun.IgniteSqlOperatorTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * Converter rule for sort operator.
 */
@Value.Enclosing
public class SortConverterRule extends RelRule<SortConverterRule.Config> {
    public static final RelOptRule INSTANCE = SortConverterRule.Config.DEFAULT.toRule();

    /** Creates a LimitConverterRule. */
    private SortConverterRule(SortConverterRule.Config config) {
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

        RelTraitSet inTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single());

        RelTraitSet outTraits = inTraits
                .replace(sort.collation);

        // Pure limit case.
        if (sort.collation == RelCollations.EMPTY) {
            call.transformTo(
                    new IgniteLimit(cluster, outTraits, convert(sort.getInput(), inTraits), sort.offset, sort.fetch)
            );

            return;
        }

        // If either `fetch` or `offset` is specified, the final evaluation must occur on a single node
        // (i.e., the plan must have a `single` distribution). Unlike Limit, a Sort node can be pushed
        // below an Exchange, which means it might not inherently satisfy the requirement of single-point
        // evaluation. To address this, during conversion we construct the Sort node with a limit equal to
        // the sum of `fetch` and `offset`. This ensures that if each shard returns at least that many rows,
        // the union of all shards will contain enough data to satisfy the original request. To enforce
        // correct semantics, we place a Limit node on top with the original `fetch` and `offset` values
        // to perform the final trimming at a single point in the plan.

        RelNode result = new IgniteSort(
                cluster,
                outTraits,
                convert(sort.getInput(), inTraits),
                sort.getCollation(),
                null,
                createLimitForSort(cluster.getPlanner().getExecutor(), cluster.getRexBuilder(), sort.offset, sort.fetch)
        );

        if (sort.fetch != null || sort.offset != null) {
            result = new IgniteLimit(cluster, outTraits, convert(result, outTraits), sort.offset, sort.fetch);
        }

        call.transformTo(result);
    }

    private static @Nullable RexNode createLimitForSort(
            @Nullable RexExecutor executor, RexBuilder builder, @Nullable RexNode offset, @Nullable RexNode fetch
    ) {
        if (fetch == null) {
            // Current implementation of SortNode cannot handle offset-only case.
            return null;
        }

        if (offset != null) {
            boolean shouldTryToSimplify = RexUtil.isLiteral(fetch, false) 
                    && RexUtil.isLiteral(offset, false);

            fetch = builder.makeCall(IgniteSqlOperatorTable.PLUS, fetch, offset);

            if (shouldTryToSimplify && executor != null) {
                try {
                    List<RexNode> result = new ArrayList<>();
                    executor.reduce(builder, List.of(fetch), result);

                    assert result.size() <= 1 : result;

                    if (result.size() == 1) {
                        fetch = result.get(0);
                    }
                } catch (Exception ex) {
                    if (IgniteUtils.assertionsEnabled()) {
                        ExceptionUtils.sneakyThrow(ex);
                    }

                    // Just ignore the exception, we will deal with this expression later again,
                    // and next time we might have all the required context to evaluate it.
                }
            }
        }

        return fetch;
    }
}
