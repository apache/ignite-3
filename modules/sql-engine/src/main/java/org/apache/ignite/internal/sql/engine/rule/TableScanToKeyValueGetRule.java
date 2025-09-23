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

import static org.apache.ignite.internal.sql.engine.util.RexUtils.builder;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.sql.engine.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueGet;
import org.apache.ignite.internal.sql.engine.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.RexUtils;
import org.apache.ignite.internal.util.Pair;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * Tries to convert given scan node to physical node representing primary key lookup.
 *
 * <p>Conversion will be successful if: <ol>
 *     <li>there is condition</li>
 *     <li>table has primary key index</li>
 *     <li>condition covers all columns of primary key index</li>
 *     <li>only single search key is derived from condition</li>
 * </ol>
 */
@Value.Enclosing
public class TableScanToKeyValueGetRule extends RelRule<TableScanToKeyValueGetRule.Config> {
    public static final RelOptRule INSTANCE = Config.INSTANCE.toRule();

    private TableScanToKeyValueGetRule(Config cfg) {
        super(cfg);
    }

    /** {@inheritDoc} */
    @Override
    public void onMatch(RelOptRuleCall call) {
        IgniteLogicalTableScan scan = cast(call.rel(0));

        Pair<List<RexNode>, RexNode> expressionsAndPostFilter = analyzeCondition(scan);

        if (expressionsAndPostFilter == null || expressionsAndPostFilter.getFirst() == null) {
            return;
        }

        call.transformTo(
                new IgniteKeyValueGet(
                        scan.getCluster(),
                        scan.getTraitSet()
                                .replace(IgniteConvention.INSTANCE)
                                .replace(IgniteDistributions.single()),
                        scan.getTable(),
                        scan.getHints(),
                        expressionsAndPostFilter.getFirst(),
                        scan.fieldNames(), 
                        scan.projects(),
                        expressionsAndPostFilter.getSecond(),
                        scan.requiredColumns()
                )
        );
    }

    static @Nullable Pair<List<RexNode>, @Nullable RexNode> analyzeCondition(ProjectableFilterableTableScan scan) {
        List<SearchBounds> bounds = deriveSearchBounds(scan);

        if (nullOrEmpty(bounds)) {
            return null;
        }

        List<RexNode> expressions = new ArrayList<>(bounds.size());

        RelOptCluster cluster = scan.getCluster();
        RexBuilder rexBuilder = builder(cluster);
        RexNode originalCondition = RexUtil.toCnf(rexBuilder, scan.condition());
        Set<RexNode> condition = new HashSet<>(RelOptUtil.conjunctions(originalCondition));

        for (SearchBounds bound : bounds) {
            // iteration over a number of search keys are not supported yet,
            // thus we need to make sure only single key was derived
            if (!(bound instanceof ExactBounds)) {
                return null;
            }

            condition.remove(bound.condition());
            expressions.add(((ExactBounds) bound).bound());
        }

        if (nullOrEmpty(expressions)) {
            return null;
        }

        RexNode resultingCondition = RexUtil.composeConjunction(rexBuilder, condition);
        if (resultingCondition.isAlwaysTrue()) {
            resultingCondition = null;
        }

        return new Pair<>(expressions, resultingCondition);
    }

    private static @Nullable List<SearchBounds> deriveSearchBounds(ProjectableFilterableTableScan scan) {
        RexNode condition = scan.condition();

        if (condition == null) {
            return null;
        }

        IgniteTable table = scan.getTable().unwrap(IgniteTable.class);

        assert table != null : scan.getTable();

        IgniteIndex primaryKeyIndex = table.indexes().values().stream()
                .filter(IgniteIndex::primaryKey)
                .findAny()
                .orElse(null);

        if (primaryKeyIndex == null) {
            return null;
        }

        RelOptCluster cluster = scan.getCluster();
        RelCollation collation = primaryKeyIndex.collation();
        ImmutableIntList requiredColumns = scan.requiredColumns();
        RelDataType rowType = table.getRowType(cluster.getTypeFactory());

        if (requiredColumns != null) {
            Mappings.TargetMapping targetMapping = Commons.projectedMapping(
                    rowType.getFieldCount(), requiredColumns
            );

            RelCollation beforeTrimming = collation;

            collation = collation.apply(targetMapping);

            if (collation.getFieldCollations().size() != beforeTrimming.getFieldCollations().size()) {
                // some columns used in the key were trimmed out. There is no chance to compose
                // proper search bound
                return null;
            }
        }

        return RexUtils.buildHashSearchBounds(
                cluster,
                collation,
                condition,
                rowType,
                requiredColumns
        );
    }

    private static <T extends RelNode> T cast(RelNode node) {
        return (T) node;
    }

    /** Configuration. */
    @SuppressWarnings({"ClassNameSameAsAncestorName", "InnerClassFieldHidesOuterClassField"})
    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config INSTANCE = ImmutableTableScanToKeyValueGetRule.Config.of()
                .withDescription("TableScanToKeyValueGetRule")
                .withOperandSupplier(o0 ->
                        o0.operand(IgniteLogicalTableScan.class)
                                .predicate(scan -> scan.condition() != null)
                                .noInputs())
                .as(Config.class);

        /** {@inheritDoc} */
        @Override
        default TableScanToKeyValueGetRule toRule() {
            return new TableScanToKeyValueGetRule(this);
        }
    }
}
