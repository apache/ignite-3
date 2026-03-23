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

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteHashJoin;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * Hash join converter.
 */
public class HashJoinConverterRule extends AbstractIgniteConverterRule<LogicalJoin> {
    private static final EnumSet<JoinRelType> TYPES_SUPPORTING_NON_EQUI_CONDITIONS = EnumSet.of(
            JoinRelType.INNER, JoinRelType.SEMI, JoinRelType.LEFT
    );

    public static final RelOptRule INSTANCE = new HashJoinConverterRule();

    /**
     * Creates a converter.
     */
    private HashJoinConverterRule() {
        super(LogicalJoin.class, "HashJoinConverter");
    }

    /** {@inheritDoc} */
    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalJoin logicalJoin = call.rel(0);

        return matches(logicalJoin);
    }

    /** Returns {@code true} if this rule can be applied to given join node, returns {@code false} otherwise. */
    public static boolean matches(LogicalJoin join) {
        JoinInfo joinInfo = Commons.getNonStrictEquiJoinCondition(join);

        if (nullOrEmpty(joinInfo.pairs())) {
            return false;
        }

        List<Boolean> filterNulls = new ArrayList<>();
        RelOptUtil.splitJoinCondition(
                join.getLeft(), join.getRight(), join.getCondition(),
                new ArrayList<>(), new ArrayList<>(), filterNulls
        );

        // IS NOT DISTINCT currently not supported by HashJoin
        if (filterNulls.stream().anyMatch(filter -> !filter)) {
            return false;
        }

        //noinspection RedundantIfStatement
        if (!joinInfo.isEqui() && !TYPES_SUPPORTING_NON_EQUI_CONDITIONS.contains(join.getJoinType())) {
            // Joins which emits unmatched right part requires special handling of `nonEquiCondition`
            // on execution level. As of now it's known limitations.
            return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override
    protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalJoin rel) {
        RelOptCluster cluster = rel.getCluster();
        RelTraitSet traits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single());
        RelNode left = convert(rel.getLeft(), traits);
        RelNode right = convert(rel.getRight(), traits);

        return new IgniteHashJoin(cluster, traits, left, right, rel.getCondition(), rel.getVariablesSet(), rel.getJoinType());
    }
}
