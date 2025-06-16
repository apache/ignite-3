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
import java.util.Map;
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
        final Sort sort = call.rel(0);
        RelOptCluster cluster = sort.getCluster();

        if (sort.fetch != null || sort.offset != null) {
            RelTraitSet traits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                    .replace(sort.getCollation())
                    .replace(IgniteDistributions.single());

            if (sort.collation == RelCollations.EMPTY || sort.fetch == null) {
                call.transformTo(new IgniteLimit(cluster, traits, convert(sort.getInput(), traits), sort.offset,
                        sort.fetch));
            } else {
                RelNode igniteSort = new IgniteSort(
                        cluster,
                        cluster.traitSetOf(IgniteConvention.INSTANCE).replace(sort.getCollation()),
                        convert(sort.getInput(), cluster.traitSetOf(IgniteConvention.INSTANCE)),
                        sort.getCollation(),
                        null,
                        createLimitForSort(cluster.getPlanner().getExecutor(), cluster.getRexBuilder(), sort.offset, sort.fetch)
                );

                call.transformTo(
                        new IgniteLimit(cluster, traits, convert(igniteSort, traits), sort.offset, sort.fetch),
                        Map.of(
                                new IgniteLimit(cluster, traits, convert(sort.getInput(), traits), sort.offset, sort.fetch),
                                sort
                        )
                );
            }
        } else {
            RelTraitSet outTraits = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(sort.getCollation());
            RelTraitSet inTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
            RelNode input = convert(sort.getInput(), inTraits);

            call.transformTo(new IgniteSort(cluster, outTraits, input, sort.getCollation()));
        }
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
