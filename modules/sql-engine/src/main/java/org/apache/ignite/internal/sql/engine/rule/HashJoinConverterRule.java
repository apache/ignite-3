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
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteHashJoin;

/**
 * Hash join converter.
 */
public class HashJoinConverterRule extends AbstractIgniteConverterRule<LogicalJoin> {
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

        if (nullOrEmpty(logicalJoin.analyzeCondition().pairs())) {
            return false;
        }

        List<Boolean> filterNulls = new ArrayList<>();
        RelOptUtil.splitJoinCondition(
                logicalJoin.getLeft(), logicalJoin.getRight(), logicalJoin.getCondition(),
                new ArrayList<>(), new ArrayList<>(), filterNulls
        );

        // IS NOT DISTINCT currently not supported by HashJoin
        if (filterNulls.stream().anyMatch(filter -> !filter)) {
            return false;
        }

        //noinspection RedundantIfStatement
        if (!logicalJoin.analyzeCondition().isEqui() && logicalJoin.getJoinType().generatesNullsOnLeft()) {
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
        RelTraitSet outTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelTraitSet leftInTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelTraitSet rightInTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelNode left = convert(rel.getLeft(), leftInTraits);
        RelNode right = convert(rel.getRight(), rightInTraits);

        return new IgniteHashJoin(cluster, outTraits, left, right, rel.getCondition(), rel.getVariablesSet(), rel.getJoinType());
    }
}
