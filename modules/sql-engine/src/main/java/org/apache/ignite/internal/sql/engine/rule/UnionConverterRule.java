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
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteUnionAll;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.immutables.value.Value;

/**
 * UnionConverterRule.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
@Value.Enclosing
public class UnionConverterRule extends RelRule<UnionConverterRule.Config> {
    /** Instance. */
    public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    public UnionConverterRule(Config cfg) {
        super(cfg);
    }

    /** {@inheritDoc} */
    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalUnion union = call.rel(0);

        RelOptCluster cluster = union.getCluster();
        RelTraitSet traits = cluster.traitSetOf(IgniteConvention.INSTANCE);

        List<RelNode> inputs = Commons.transform(union.getInputs(), input -> convert(input, traits));
        List<RelNode> convertedInputs = convertInputs(cluster, inputs, traits);

        RelNode res = new IgniteUnionAll(cluster, traits, convertedInputs);

        if (!union.all) {
            RelBuilder relBuilder = relBuilderFactory.create(union.getCluster(), null);

            relBuilder
                    .push(res)
                    .aggregate(relBuilder.groupKey(ImmutableBitSet.range(union.getRowType().getFieldCount())));

            res = convert(relBuilder.build(), union.getTraitSet());
        }

        call.transformTo(res);
    }

    private static List<RelNode> convertInputs(RelOptCluster cluster, List<RelNode> inputs, RelTraitSet traits) {
        List<RelDataType> inputRowTypes = inputs.stream()
                .map(RelNode::getRowType)
                .collect(Collectors.toList());

        // Output type of a set operator is equal to leastRestrictive(inputTypes) (see SetOp::deriveRowType)

        RelDataType resultType = cluster.getTypeFactory().leastRestrictive(inputRowTypes);
        if (resultType == null) {
            throw new IllegalArgumentException("Cannot compute compatible row type for arguments to set op: " + inputRowTypes);
        }

        // Check output type of each input, if input's type does not match the result type,
        // then add a projection with casts for non-matching fields.

        RexBuilder rexBuilder = cluster.getRexBuilder();
        List<RelNode> actualInputs = new ArrayList<>(inputs.size());

        for (RelNode input : inputs) {
            RelDataType inputRowType = input.getRowType();

            if (!resultType.equalsSansFieldNames(inputRowType)) {
                List<RexNode> exprs = new ArrayList<>(inputRowType.getFieldCount());

                for (int i = 0; i < resultType.getFieldCount(); i++) {
                    RelDataType fieldType = inputRowType.getFieldList().get(i).getType();
                    RelDataType outFieldType = resultType.getFieldList().get(i).getType();
                    RexNode ref = rexBuilder.makeInputRef(input, i);

                    if (fieldType.equals(outFieldType)) {
                        exprs.add(ref);
                    } else {
                        RexNode expr = rexBuilder.makeCast(outFieldType, ref, true, false);
                        exprs.add(expr);
                    }
                }

                actualInputs.add(new IgniteProject(cluster, traits, input, exprs, resultType));
            } else {
                actualInputs.add(input);
            }
        }

        return actualInputs;
    }

    /**
     * Config interface.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        UnionConverterRule.Config DEFAULT = ImmutableUnionConverterRule.Config.of()
                .withDescription("UnionConverterRule")
                .withOperandFor(LogicalUnion.class);

        /** Defines an operand tree for the given classes. */
        default UnionConverterRule.Config withOperandFor(Class<? extends LogicalUnion> union) {
            return withOperandSupplier(
                    o0 -> o0.operand(union).anyInputs()
            )
                    .as(UnionConverterRule.Config.class);
        }

        /** {@inheritDoc} */
        @Override
        default UnionConverterRule toRule() {
            return new UnionConverterRule(this);
        }
    }
}
