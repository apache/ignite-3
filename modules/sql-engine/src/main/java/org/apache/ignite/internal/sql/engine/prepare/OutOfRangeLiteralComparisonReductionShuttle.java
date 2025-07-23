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

package org.apache.ignite.internal.sql.engine.prepare;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.RangeSet;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.Sarg;
import org.apache.ignite.internal.sql.engine.sql.fun.IgniteSqlOperatorTable;
import org.apache.ignite.internal.sql.engine.util.RexUtils;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;

/**
 * A {@link RexShuttle} that simplifies binary comparison expressions involving casts and out-of-range literals in numeric comparisons.
 *
 * <p>This shuttle looks for patterns like:
 * <pre>
 *     CAST(expression AS widerType) &lt;op&gt; literal
 * </pre>
 *
 * <p>and attempts to reduce them when the literal falls outside the valid bounds of the original (uncast) numeric type of
 * {@code expression}. For example, if comparing an {@code INTEGER} expression casted to {@code BIGINT} against a literal that exceeds
 * {@code Integer.MAX_VALUE}, the expression can be safely reduced to {@code FALSE} or a simpler form.
 */
public class OutOfRangeLiteralComparisonReductionShuttle extends RexShuttle {
    private final RexBuilder builder;

    private int comparisonOpTracker;

    /** Constructs the object. */
    public OutOfRangeLiteralComparisonReductionShuttle(RexBuilder builder) {
        this.builder = builder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
        if (RexUtils.isBinaryComparison(call)) {
            comparisonOpTracker++;
            try {
                RexNode firstOp = call.getOperands().get(0).accept(this);
                RexNode secondOp = call.getOperands().get(1).accept(this);

                RexNode expression = null;
                RexNode possibleLiteral = null;
                SqlKind op = call.op.getKind();

                // We are looking for pattern like this:
                //      CAST(<expression> AS <wider type>) <op> <possibleLiteral>
                if (RexUtils.isLosslessCast(firstOp)) {
                    expression = ((RexCall) firstOp).getOperands().get(0);
                    possibleLiteral = secondOp;
                } else if (RexUtils.isLosslessCast(secondOp)) {
                    expression = ((RexCall) secondOp).getOperands().get(0);
                    possibleLiteral = firstOp;
                    op = op.reverse();
                }

                RelDataType probingType = expression != null ? expression.getType() : null;

                if (probingType != null && probingType.getFamily() == SqlTypeFamily.NUMERIC) {
                    assert expression != null && possibleLiteral != null;

                    if (possibleLiteral instanceof RexLiteral) {
                        BigDecimal lower = TypeUtils.lowerBoundFor(probingType);
                        BigDecimal upper = TypeUtils.upperBoundFor(probingType);

                        BigDecimal current;
                        try {
                            // NumberFormatException may be thrown if current value is Double.INF.
                            current = ((RexLiteral) possibleLiteral).getValueAs(BigDecimal.class);
                        } catch (NumberFormatException ignored) {
                            current = null;
                        }

                        if (lower != null && upper != null && current != null) {
                            // Operation type was normalized to the form <expression> <op> <literal>.
                            switch (op) {
                                case EQUALS:
                                case IS_NOT_DISTINCT_FROM:
                                    if (inRange(current, lower, upper)) {
                                        if (probingType.getScale() >= current.stripTrailingZeros().scale()) {
                                            // Required scale covers actual scale of the literal provided, hence we must compare
                                            // the result of expression which is known in runtime only.
                                            return call.clone(call.getType(), List.of(
                                                    expression, builder.makeLiteral(current, probingType)
                                            ));
                                        }
                                    }

                                    return builder.makeLiteral(false);

                                case GREATER_THAN:
                                case GREATER_THAN_OR_EQUAL:
                                    if (inRange(current, lower, upper)) {
                                        // If we are going to lose some precision, we might need to adjust operation as well.
                                        // Here are few examples:
                                        //      Values allowed by the type: -128, -127, -126, etc
                                        //      If comparison is `x >= -127.5`, we may simply truncate the literal as `-127` is the closest
                                        //      valid value allowed by the type. But if comparison will be `x > -127.5`, we should adjust
                                        //      the operation in order to include the bound: `x >= -127`. Otherwise, value `-127` will be
                                        //      excluded from result, which makes this transformation not equivalent. The `signum` is
                                        //      checked to make the adjustment in the right direction, because digits after decimal point
                                        //      is simply truncated. In case of positive values it make the value lesser than original,
                                        //      but in case of negative values it make the value greater.
                                        if (probingType.getScale() < current.stripTrailingZeros().scale()) {
                                            if (current.signum() < 0) {
                                                return builder.makeCall(
                                                        IgniteSqlOperatorTable.GREATER_THAN_OR_EQUAL,
                                                        expression,
                                                        builder.makeLiteral(current, probingType)
                                                );
                                            } else if (current.signum() > 0) {
                                                return builder.makeCall(
                                                        IgniteSqlOperatorTable.GREATER_THAN,
                                                        expression,
                                                        builder.makeLiteral(current, probingType)
                                                );
                                            }
                                        }

                                        return call.clone(call.getType(), List.of(
                                                expression, builder.makeLiteral(current, probingType)
                                        ));
                                    }

                                    if (current.compareTo(upper) > 0) {
                                        // There is no value which is greater than the upper bound for the type.
                                        return builder.makeLiteral(false);
                                    }

                                    // If current comparison covers all the possible values, no need to do the comparison,
                                    // we only need to make sure the expressions is not evaluated to null.
                                    return builder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, expression);

                                case LESS_THAN:
                                case LESS_THAN_OR_EQUAL:
                                    if (inRange(current, lower, upper)) {
                                        // If we are going to lose some precision, we might need to adjust operation as well.
                                        // Here are few examples:
                                        //      Values allowed by the type: -128, -127, -126, etc
                                        //      If comparison is `x >= -127.5`, we may simply truncate the literal as `-127` is the closest
                                        //      valid value allowed by the type. But if comparison will be `x > -127.5`, we should adjust
                                        //      the operation in order to include the bound: `x >= -127`. Otherwise, value `-127` will be
                                        //      excluded from result, which makes this transformation not equivalent. The `signum` is
                                        //      checked to make the adjustment in the right direction, because digits after decimal point
                                        //      is simply truncated. In case of positive values it make the value lesser than original,
                                        //      but in case of negative values it make the value greater.
                                        if (probingType.getScale() < current.stripTrailingZeros().scale()) {
                                            if (current.signum() < 0) {
                                                return builder.makeCall(
                                                        IgniteSqlOperatorTable.LESS_THAN,
                                                        expression,
                                                        builder.makeLiteral(current, probingType)
                                                );
                                            } else if (current.signum() > 0) {
                                                return builder.makeCall(
                                                        IgniteSqlOperatorTable.LESS_THAN_OR_EQUAL,
                                                        expression,
                                                        builder.makeLiteral(current, probingType)
                                                );
                                            }
                                        }

                                        return call.clone(call.getType(), List.of(
                                                expression, builder.makeLiteral(current, probingType)
                                        ));
                                    }

                                    if (current.compareTo(lower) < 0) {
                                        // There is no value which is less than the lower bound for the type.
                                        return builder.makeLiteral(false);
                                    }

                                    // If current comparison covers all the possible values, no need to do the comparison,
                                    // we only need to make sure the expressions is not evaluated to null.
                                    return builder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, expression);

                                default:
                                    // NO-OP
                            }
                        }
                    }
                }

                return call.getOperands().get(0) != firstOp || call.getOperands().get(1) != secondOp
                        ? call.clone(call.getType(), List.of(firstOp, secondOp))
                        : call;
            } finally {
                comparisonOpTracker--;
            }
        }

        if (call.isA(SqlKind.SEARCH)) {
            RexNode ref = call.getOperands().get(0);
            if (RexUtils.isLosslessCast(ref) && ref.getType().getFamily() == SqlTypeFamily.NUMERIC) {
                ref = ((RexCall) ref).getOperands().get(0);
                RelDataType probingType = ref.getType();

                @SuppressWarnings("unchecked")
                Sarg<BigDecimal> values = ((RexLiteral) call.getOperands().get(1)).getValueAs(Sarg.class);

                assert values != null;

                if (values.isPoints()) {
                    BigDecimal lower = TypeUtils.lowerBoundFor(probingType);
                    BigDecimal upper = TypeUtils.upperBoundFor(probingType);

                    assert lower != null;
                    assert upper != null;

                    RangeSet<?> normalized = ImmutableRangeSet.copyOf(
                            values.rangeSet.asRanges().stream()
                                    .filter(r -> {
                                        BigDecimal current = r.lowerEndpoint();

                                        return inRange(current, lower, upper)
                                                && probingType.getScale() >= current.stripTrailingZeros().scale();

                                    })
                                    .collect(Collectors.toList())
                    );

                    //noinspection unchecked
                    RexLiteral normalizedSargLiteral = builder.makeSearchArgumentLiteral(Sarg.of(values.nullAs, normalized), probingType);

                    return builder.makeCall(
                            SqlStdOperatorTable.SEARCH, ref, normalizedSargLiteral
                    );
                }
            }
        }

        // This shuttle is supposed to adjust binary comparisons only, hence if we are not in the context of comparison,
        // we skip cast simplification as it may change type of the expressions (e.g. CAST to nullable type will become non-nullable).
        if (comparisonOpTracker > 0 && RexUtils.isLosslessCast(call) && call.getOperands().get(0) instanceof RexLiteral) {
            return builder.makeLiteral(((RexLiteral) call.getOperands().get(0)).getValue(), call.getType());
        }

        return RexUtil.flatten(builder, super.visitCall(call));
    }

    private static boolean inRange(BigDecimal val, BigDecimal lower, BigDecimal upper) {
        return lower.compareTo(val) <= 0 && upper.compareTo(val) >= 0;
    }
}
