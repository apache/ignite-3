/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.sql.engine.schema.InternalIgniteTable;
import org.immutables.value.Value;

/**
 * ExposeIndexRule.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
@Value.Enclosing
public class ExposeIndexRule extends RelRule<ExposeIndexRule.Config> {
    public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    public ExposeIndexRule(Config config) {
        super(config);
    }

    private static boolean preMatch(IgniteLogicalTableScan scan) {
        // has indexes to expose
        return !scan.getTable().unwrap(InternalIgniteTable.class).indexes().isEmpty();
    }

    /** {@inheritDoc} */
    @Override
    public void onMatch(RelOptRuleCall call) {
        IgniteLogicalTableScan scan = call.rel(0);
        RelOptCluster cluster = scan.getCluster();

        RelOptTable optTable = scan.getTable();
        InternalIgniteTable igniteTable = optTable.unwrap(InternalIgniteTable.class);
        List<RexNode> proj = scan.projects();
        RexNode condition = scan.condition();
        ImmutableBitSet requiredCols = scan.requiredColumns();

        List<IgniteLogicalIndexScan> indexes = igniteTable.indexes().keySet().stream()
                .map(idxName -> igniteTable.toRel(cluster, optTable, idxName, proj, condition, requiredCols))
                .collect(Collectors.toList());

        if (indexes.isEmpty()) {
            return;
        }

        Map<RelNode, RelNode> equivMap = new HashMap<>(indexes.size());
        for (int i = 1; i < indexes.size(); i++) {
            equivMap.put(indexes.get(i), scan);
        }

        call.transformTo(indexes.get(0), equivMap);
    }

    /**
     * Rule's configuration.
     */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableExposeIndexRule.Config.of()
                .withOperandSupplier(b ->
                                b.operand(IgniteLogicalTableScan.class)
                                        .predicate(ExposeIndexRule::preMatch)
                                        .anyInputs());

        /** {@inheritDoc} */
        @Override default ExposeIndexRule toRule() {
            return new ExposeIndexRule(this);
        }
    }
}
