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
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;

/**
 * Ignite Join converter.
 */
public class NestedLoopJoinConverterRule extends AbstractIgniteConverterRule<LogicalJoin> {
    public static final RelOptRule INSTANCE = new NestedLoopJoinConverterRule();

    /**
     * Creates a converter.
     */
    public NestedLoopJoinConverterRule() {
        super(LogicalJoin.class, "NestedLoopJoinConverter");
    }

    /** {@inheritDoc} */
    @Override
    protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalJoin rel) {
        boolean hasHashJoinRule = planner.getRules().contains(HashJoinConverterRule.INSTANCE);

        if (hasHashJoinRule && HashJoinConverterRule.matches(rel)) {
            return null;
        }

        RelOptCluster cluster = rel.getCluster();
        RelTraitSet traits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single());
        RelNode left = convert(rel.getLeft(), traits);
        RelNode right = convert(rel.getRight(), traits);

        return new IgniteNestedLoopJoin(cluster, traits, left, right, rel.getCondition(), rel.getVariablesSet(), rel.getJoinType());
    }
}
