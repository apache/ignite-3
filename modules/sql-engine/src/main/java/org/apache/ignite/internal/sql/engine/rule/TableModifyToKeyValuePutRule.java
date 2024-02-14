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
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitor;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify.Operation;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.immutables.value.Value;

/**
 * Rule that converts {@link TableModify} representing INSERT operation with a determined source
 * to a Key-Value PUT operation.
 *
 * <p>Note: at the moment, this rule support only single row insert.
 */
@Value.Enclosing
public class TableModifyToKeyValuePutRule extends RelRule<TableModifyToKeyValuePutRule.Config> {
    public static final RelOptRule VALUES = Config.VALUES.toRule();
    public static final RelOptRule PROJECT = Config.PROJECT.toRule();

    private TableModifyToKeyValuePutRule(Config cfg) {
        super(cfg);
    }

    /** {@inheritDoc} */
    @Override
    public void onMatch(RelOptRuleCall call) {
        List<RelNode> operands = call.getRelList();

        TableModify modify = cast(operands.get(0));

        assert modify.getOperation() == TableModify.Operation.INSERT : modify.getOperation();

        List<RexNode> expressions;
        if (operands.size() == 2) {
            Values values = cast(operands.get(1));

            assert values.getTuples().size() == 1 : "Expected exactly one tuple, but was " + values.getTuples().size();

            expressions = List.copyOf(values.getTuples().get(0));
        } else {
            assert operands.size() == 3 : operands;

            Values values = cast(operands.get(2));

            assert values.getTuples().size() == 1 : "Expected exactly one tuple, but was " + values.getTuples().size();

            List<RexNode> inputExpressions = List.copyOf(values.getTuples().get(0));

            RexVisitor<RexNode> inputInliner = new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef inputRef) {
                    return inputExpressions.get(inputRef.getIndex());
                }
            };

            Project project = cast(operands.get(1));

            expressions = inputInliner.visitList(project.getProjects());
        }

        call.transformTo(
                new IgniteKeyValueModify(
                        modify.getCluster(),
                        modify.getTraitSet()
                                .replace(IgniteConvention.INSTANCE)
                                .replace(IgniteDistributions.single()),
                        modify.getTable(),
                        Operation.PUT,
                        expressions
                )
        );
    }

    private static <T extends RelNode> T cast(RelNode node) {
        return (T) node;
    }

    /**
     * Configuration.
     */
    @SuppressWarnings({"ClassNameSameAsAncestorName", "InnerClassFieldHidesOuterClassField"})
    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config VALUES = ImmutableTableModifyToKeyValuePutRule.Config.of()
                .withDescription("TableModifyToKeyValuePutRule:VALUES")
                .withOperandSupplier(o0 ->
                        o0.operand(TableModify.class)
                                .predicate(TableModify::isInsert)
                                .oneInput(o1 ->
                                        o1.operand(Values.class)
                                                .predicate(values -> values.getTuples().size() == 1)
                                                .noInputs()))
                .as(Config.class);

        Config PROJECT = ImmutableTableModifyToKeyValuePutRule.Config.of()
                .withDescription("TableModifyToKeyValuePutRule:PROJECT")
                .withOperandSupplier(o0 ->
                        o0.operand(TableModify.class)
                                .predicate(TableModify::isInsert)
                                .oneInput(o1 ->
                                        o1.operand(Project.class).oneInput(o2 ->
                                                o2.operand(Values.class)
                                                        .predicate(values -> values.getTuples().size() == 1)
                                                        .noInputs())))
                .as(Config.class);

        /** {@inheritDoc} */
        @Override
        default TableModifyToKeyValuePutRule toRule() {
            return new TableModifyToKeyValuePutRule(this);
        }
    }
}
