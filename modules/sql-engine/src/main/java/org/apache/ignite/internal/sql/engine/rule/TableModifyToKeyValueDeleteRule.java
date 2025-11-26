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

import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify.Operation;
import org.apache.ignite.internal.sql.engine.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.util.Pair;
import org.immutables.value.Value;

/**
 * Rule that converts {@link TableModify} representing DELETE operation with a determined source
 * to a Key-Value DELETE operation.
 *
 * <p>Conversion will be successful if: <ol>
 *     <li>there is condition</li>
 *     <li>table has primary key index (i.e. there is an index for which {@link IgniteIndex#primaryKey()} returns {@code true})</li>
 *     <li>condition covers all columns of primary key index</li>
 *     <li>condition doesn't involve other columns</li>
 *     <li>only single search key is derived from condition</li>
 * </ol>
 */
@Value.Enclosing
public class TableModifyToKeyValueDeleteRule extends RelRule<TableModifyToKeyValueDeleteRule.Config> {
    public static final RelOptRule INSTANCE = Config.INSTANCE.toRule();

    private TableModifyToKeyValueDeleteRule(Config cfg) {
        super(cfg);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        TableModify modify = call.rel(0);
        TableScan scan = call.rel(1);

        RelOptTable relTableToModify = modify.getTable();
        RelOptTable relTableToScan = scan.getTable();

        assert relTableToModify != null;
        assert relTableToScan != null;

        IgniteTable igniteTableToModify = relTableToModify.unwrap(IgniteTable.class);
        IgniteTable igniteTableToScan = relTableToScan.unwrap(IgniteTable.class);
        return igniteTableToModify != null
                && igniteTableToScan != null
                && igniteTableToModify.id() == igniteTableToScan.id();
    }

    /** {@inheritDoc} */
    @Override
    public void onMatch(RelOptRuleCall call) {
        ProjectableFilterableTableScan scan = call.rel(1);

        Pair<List<RexNode>, RexNode> expressionsAndPostFilter = TableScanToKeyValueGetRule.analyzeCondition(scan);

        if (expressionsAndPostFilter == null || expressionsAndPostFilter.getFirst() == null) {
            return;
        }

        // post-condition is not allowed.
        if (expressionsAndPostFilter.getSecond() != null) {
            return;
        }

        call.transformTo(
                new IgniteKeyValueModify(
                        scan.getCluster(),
                        scan.getTraitSet()
                                .replace(IgniteConvention.INSTANCE)
                                .replace(IgniteDistributions.single()),
                        scan.getTable(),
                        Operation.DELETE,
                        expressionsAndPostFilter.getFirst()
                )
        );
    }

    /**
     * Configuration.
     */
    @SuppressWarnings({"ClassNameSameAsAncestorName", "InnerClassFieldHidesOuterClassField"})
    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config INSTANCE = ImmutableTableModifyToKeyValueDeleteRule.Config.of()
                .withDescription("TableModifyToKeyValueDeleteRule")
                .withOperandSupplier(o0 ->
                        o0.operand(TableModify.class)
                                .predicate(TableModify::isDelete)
                                .oneInput(o1 ->
                                        o1.operand(ProjectableFilterableTableScan.class)
                                                .predicate(scan -> scan.condition() != null)
                                                .noInputs()))
                .as(Config.class);

        /** {@inheritDoc} */
        @Override
        default TableModifyToKeyValueDeleteRule toRule() {
            return new TableModifyToKeyValueDeleteRule(this);
        }
    }
}
