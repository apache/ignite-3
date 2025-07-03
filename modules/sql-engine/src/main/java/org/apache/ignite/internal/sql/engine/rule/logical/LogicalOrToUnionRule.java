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

package org.apache.ignite.internal.sql.engine.rule.logical;

import static org.apache.ignite.internal.sql.engine.util.RexUtils.tryToDnf;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.util.CollectionUtils;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * Converts OR to UNION ALL.
 */
@Value.Enclosing
public class LogicalOrToUnionRule extends RelRule<LogicalOrToUnionRule.Config> {
    /** Instance. */
    public static final RelOptRule INSTANCE = new LogicalOrToUnionRule(Config.SCAN);

    /**
     * Constructor.
     *
     * @param config Rule configuration.
     */
    private LogicalOrToUnionRule(Config config) {
        super(config);
    }

    private static @Nullable List<RexNode> getOrOperands(RexBuilder rexBuilder, RexNode condition) {
        RexNode dnf = tryToDnf(rexBuilder, condition, 2);

        if (dnf != null && !dnf.isA(SqlKind.OR)) {
            return null;
        }

        List<RexNode> operands = RelOptUtil.disjunctions(dnf);
        assert operands.size() <= 2 : "unexpected operands count: " + operands.size();

        if (operands.size() != 2 || RexUtil.find(SqlKind.IS_NULL).anyContain(operands)) {
            return null;
        }

        return operands;
    }

    private void buildInput(RelBuilder relBldr, RelNode input, RexNode condition) {
        IgniteLogicalTableScan scan = (IgniteLogicalTableScan) input;

        // Set default traits, real traits will be calculated for physical node.
        RelTraitSet trait = scan.getCluster().traitSet();

        relBldr.push(IgniteLogicalTableScan.create(
                scan.getCluster(),
                trait,
                scan.getHints(),
                scan.getTable(),
                scan.fieldNames(),
                scan.projects(),
                condition,
                scan.requiredColumns()
        ));
    }

    /**
     * Creates 'UnionAll' for conditions.
     *
     * @param cluster The cluster UnionAll expression will belongs to.
     * @param input   Input.
     * @param op1     First filter condition.
     * @param op2     Second filter condition.
     * @return UnionAll expression.
     */
    private RelNode createUnionAll(RelOptCluster cluster, RelNode input, RexNode op1, RexNode op2) {
        RelBuilder relBldr = relBuilderFactory.create(cluster, null);

        buildInput(relBldr, input, op1);
        buildInput(relBldr, input, relBldr.and(op2, relBldr.or(relBldr.isNull(op1), relBldr.not(op1))));

        return relBldr
                .union(true)
                .build();
    }

    private RexNode getCondition(RelOptRuleCall call) {
        final IgniteLogicalTableScan rel = call.rel(0);

        return rel.condition();
    }

    /**
     * Compares intersection (currently beginning position) of condition and index fields.
     * This rule need to be triggered only if appropriate indexes will be found otherwise it`s not applicable.
     *
     * @param call Set of appropriate RelNode.
     * @param operands Operands from OR expression.
     */
    private boolean idxCollationCheck(RelOptRuleCall call, List<RexNode> operands) {
        final IgniteLogicalTableScan scan = call.rel(0);

        IgniteTable tbl = scan.getTable().unwrap(IgniteTable.class);
        IgniteTypeFactory typeFactory = Commons.typeFactory(scan.getCluster());
        int fieldCnt = tbl.getRowType(typeFactory).getFieldCount();

        BitSet idxsFirstFields = new BitSet(fieldCnt);

        for (IgniteIndex idx : tbl.indexes().values()) {
            List<RelFieldCollation> fieldCollations = idx.collation().getFieldCollations();

            if (!CollectionUtils.nullOrEmpty(fieldCollations)) {
                idxsFirstFields.set(fieldCollations.get(0).getFieldIndex());
            }
        }

        Mappings.TargetMapping mapping = scan.requiredColumns() == null
                ? Mappings.createIdentity(fieldCnt)
                : Commons.projectedMapping(fieldCnt, scan.requiredColumns());

        for (RexNode op : operands) {
            BitSet conditionFields = new BitSet(fieldCnt);

            new RexShuttle() {
                @Override public RexNode visitLocalRef(RexLocalRef inputRef) {
                    conditionFields.set(mapping.getSourceOpt(inputRef.getIndex()));
                    return inputRef;
                }
            }.apply(op);

            if (!conditionFields.intersects(idxsFirstFields)) {
                return false;
            }
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        final RelOptCluster cluster = call.rel(0).getCluster();

        List<RexNode> operands = getOrOperands(cluster.getRexBuilder(), getCondition(call));

        if (operands == null) {
            return;
        }

        if (!idxCollationCheck(call, operands)) {
            return;
        }

        RelNode input = call.rel(0);

        RelNode rel0 = createUnionAll(cluster, input, operands.get(0), operands.get(1));
        RelNode rel1 = createUnionAll(cluster, input, operands.get(1), operands.get(0));

        call.transformTo(rel0, Map.of(rel1, rel0));
    }

    /**
     * Config interface.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @Value.Immutable(singleton = false)
    public interface Config extends RuleFactoryConfig<Config> {
        Config SCAN = ImmutableLogicalOrToUnionRule.Config.builder()
                .withRuleFactory(LogicalOrToUnionRule::new)
                .withDescription("ScanLogicalOrToUnionRule")
                .withOperandSupplier(o -> o.operand(IgniteLogicalTableScan.class)
                        .predicate(scan -> scan.condition() != null)
                        .noInputs())
                .build();
    }
}
