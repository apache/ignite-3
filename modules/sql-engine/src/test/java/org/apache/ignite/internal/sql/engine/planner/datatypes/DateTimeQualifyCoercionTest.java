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

package org.apache.ignite.internal.sql.engine.planner.datatypes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.DatetimePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.NumericPair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of tests to verify behavior of type coercion for quantify operators (ALL, SOME), when operands belong to the DATETIME types.
 *
 * <p>This tests aim to help to understand in which cases implicit cast will be added to which operand.
 */
public class DateTimeQualifyCoercionTest extends BaseTypeCoercionTest {

    private static Stream<Arguments> args() {
        return Stream.of(
                forTypePair(DatetimePair.DATE_DATE)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                // TIME 0
                forTypePair(DatetimePair.TIME_0_TIME_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIME_0_TIME_1)
                        .firstOpMatches(ofType(Types.TIME_1))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIME_0_TIME_9)
                        .firstOpMatches(ofType(Types.TIME_9))
                        .secondOpBeSame(),

                // TIME 1

                forTypePair(DatetimePair.TIME_1_TIME_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIME_1_TIME_0)
                        .firstOpMatches(ofType(Types.TIME_1))
                        .secondOpMatches(ofType(Types.TIME_1)),
                forTypePair(DatetimePair.TIME_1_TIME_9)
                        .firstOpMatches(ofType(Types.TIME_9))
                        .secondOpBeSame(),

                // TIME 3

                forTypePair(DatetimePair.TIME_9_TIME_9)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIME_9_TIME_0)
                        .firstOpMatches(ofType(Types.TIME_9))
                        .secondOpMatches(ofType(Types.TIME_9)),
                forTypePair(DatetimePair.TIME_9_TIME_1)
                        .firstOpMatches(ofType(Types.TIME_9))
                        .secondOpMatches(ofType(Types.TIME_9)),

                // TIMESTAMP 0

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_1)
                        .firstOpMatches(ofType(Types.TIMESTAMP_1))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_9)
                        .firstOpMatches(ofType(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_0)
                        .firstOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_0), ofType(Types.TIMESTAMP_0)))
                        .secondOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_0), ofType(Types.TIMESTAMP_0))),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_1)
                        .firstOpMatches(anyOf(ofType(Types.TIMESTAMP_0), ofType(Types.TIMESTAMP_WLTZ_1)))
                        .secondOpMatches(anyOf(ofType(Types.TIMESTAMP_0), ofType(Types.TIMESTAMP_WLTZ_1))),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(anyOf(ofType(Types.TIMESTAMP_0), ofType(Types.TIMESTAMP_WLTZ_9)))
                        .secondOpMatches(anyOf(ofType(Types.TIMESTAMP_0), ofType(Types.TIMESTAMP_WLTZ_9))),


                // TIMESTAMP 1

                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_0)
                        .firstOpMatches(ofType(Types.TIMESTAMP_1))
                        .secondOpMatches(ofType(Types.TIMESTAMP_1)),
                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_9)
                        .firstOpMatches(ofType(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_0)
                        .firstOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_0), ofType(Types.TIMESTAMP_1)))
                        .secondOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_0), ofType(Types.TIMESTAMP_1))),
                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_1)
                        .firstOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_1), ofType(Types.TIMESTAMP_1)))
                        .secondOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_1), ofType(Types.TIMESTAMP_1))),
                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_9), ofType(Types.TIMESTAMP_1)))
                        .secondOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_9), ofType(Types.TIMESTAMP_1))),

                // TIMESTAMP 3

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_9)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_0)
                        .firstOpMatches(ofType(Types.TIMESTAMP_9))
                        .secondOpMatches(ofType(Types.TIMESTAMP_9)),
                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_1)
                        .firstOpMatches(ofType(Types.TIMESTAMP_9))
                        .secondOpMatches(ofType(Types.TIMESTAMP_9)),
                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_0)
                        .firstOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_0), ofType(Types.TIMESTAMP_9)))
                        .secondOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_0), ofType(Types.TIMESTAMP_9))),
                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_1)
                        .firstOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_1), ofType(Types.TIMESTAMP_9)))
                        .secondOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_1), ofType(Types.TIMESTAMP_9))),
                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_9), ofType(Types.TIMESTAMP_9)))
                        .secondOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_9), ofType(Types.TIMESTAMP_9))),


                // TIMESTAMP LTZ 0

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_1)
                        .firstOpMatches(ofType(Types.TIMESTAMP_WLTZ_1))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofType(Types.TIMESTAMP_WLTZ_9))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_0)
                        .firstOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_0), ofType(Types.TIMESTAMP_0)))
                        .secondOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_0), ofType(Types.TIMESTAMP_0))),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_1)
                        .firstOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_0), ofType(Types.TIMESTAMP_1)))
                        .secondOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_0), ofType(Types.TIMESTAMP_1))),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_9)
                        .firstOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_0), ofType(Types.TIMESTAMP_9)))
                        .secondOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_0), ofType(Types.TIMESTAMP_9))),

                // TIMESTAMP LTZ 1

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_0)
                        .firstOpMatches(ofType(Types.TIMESTAMP_WLTZ_1))
                        .secondOpMatches(ofType(Types.TIMESTAMP_WLTZ_1)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofType(Types.TIMESTAMP_WLTZ_9))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_0)
                        .firstOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_1), ofType(Types.TIMESTAMP_0)))
                        .secondOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_1), ofType(Types.TIMESTAMP_0))),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_1)
                        .firstOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_1), ofType(Types.TIMESTAMP_1)))
                        .secondOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_1), ofType(Types.TIMESTAMP_1))),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_9)
                        .firstOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_1), ofType(Types.TIMESTAMP_9)))
                        .secondOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_1), ofType(Types.TIMESTAMP_9))),

                // TIMESTAMP LTZ 3

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_0)
                        .firstOpMatches(ofType(Types.TIMESTAMP_WLTZ_9))
                        .secondOpMatches(ofType(Types.TIMESTAMP_WLTZ_9)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_1)
                        .firstOpMatches(ofType(Types.TIMESTAMP_WLTZ_9))
                        .secondOpMatches(ofType(Types.TIMESTAMP_WLTZ_9)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_0)
                        .firstOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_9), ofType(Types.TIMESTAMP_0)))
                        .secondOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_9), ofType(Types.TIMESTAMP_0))),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_1)
                        .firstOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_9), ofType(Types.TIMESTAMP_1)))
                        .secondOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_9), ofType(Types.TIMESTAMP_1))),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_9)
                        .firstOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_9), ofType(Types.TIMESTAMP_9)))
                        .secondOpMatches(anyOf(ofType(Types.TIMESTAMP_WLTZ_9), ofType(Types.TIMESTAMP_9)))
        );
    }

    @ParameterizedTest
    @MethodSource("args")
    public void equalsTo(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        assertPlan("SELECT c1 = SOME(SELECT c2 FROM t) FROM t", schema,
                equalsToSomeOperatorMatcher(firstOperandMatcher, secondOperandMatcher)::matches, List.of());
        assertPlan("SELECT c2 = SOME(SELECT c1 FROM t) FROM t", schema,
                equalsToSomeOperatorMatcher(secondOperandMatcher, firstOperandMatcher)::matches, List.of());
        assertPlan("SELECT c1 = ALL(SELECT c2 FROM t) FROM t", schema,
                quantifyOperatorMatcher(firstOperandMatcher, secondOperandMatcher, 0, 3)::matches, List.of());
        assertPlan("SELECT c2 = ALL(SELECT c1 FROM t) FROM t", schema,
                quantifyOperatorMatcher(firstOperandMatcher, secondOperandMatcher, 0, 3)::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("args")
    public void lessThan(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        assertPlan("SELECT c1 < SOME(SELECT c2 FROM t) FROM t", schema,
                quantifyOperatorMatcher(firstOperandMatcher, secondOperandMatcher, 0, 1)::matches, List.of());
        assertPlan("SELECT c2 < SOME(SELECT c1 FROM t) FROM t", schema,
                quantifyOperatorMatcher(secondOperandMatcher, firstOperandMatcher, 0, 1)::matches, List.of());
        assertPlan("SELECT c1 < ALL(SELECT c2 FROM t) FROM t", schema,
                quantifyOperatorMatcher(firstOperandMatcher, secondOperandMatcher, 0, 1)::matches, List.of());
        assertPlan("SELECT c2 < ALL(SELECT c1 FROM t) FROM t", schema,
                quantifyOperatorMatcher(firstOperandMatcher, secondOperandMatcher, 0, 1)::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("args")
    public void lessThanOrEqual(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        assertPlan("SELECT c1 <= SOME(SELECT c2 FROM t) FROM t", schema,
                quantifyOperatorMatcher(firstOperandMatcher, secondOperandMatcher, 0, 1)::matches, List.of());
        assertPlan("SELECT c2 <= SOME(SELECT c1 FROM t) FROM t", schema,
                quantifyOperatorMatcher(secondOperandMatcher, firstOperandMatcher, 0, 1)::matches, List.of());
        assertPlan("SELECT c1 <= ALL(SELECT c2 FROM t) FROM t", schema,
                quantifyOperatorMatcher(firstOperandMatcher, secondOperandMatcher, 0, 1)::matches, List.of());
        assertPlan("SELECT c2 <= ALL(SELECT c1 FROM t) FROM t", schema,
                quantifyOperatorMatcher(firstOperandMatcher, secondOperandMatcher, 0, 1)::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("args")
    public void greaterThan(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        assertPlan("SELECT c1 > SOME(SELECT c2 FROM t) FROM t", schema,
                quantifyOperatorMatcher(firstOperandMatcher, secondOperandMatcher, 0, 1)::matches, List.of());
        assertPlan("SELECT c2 > SOME(SELECT c1 FROM t) FROM t", schema,
                quantifyOperatorMatcher(secondOperandMatcher, firstOperandMatcher, 0, 1)::matches, List.of());
        assertPlan("SELECT c1 > ALL(SELECT c2 FROM t) FROM t", schema,
                quantifyOperatorMatcher(firstOperandMatcher, secondOperandMatcher, 0, 1)::matches, List.of());
        assertPlan("SELECT c2 > ALL(SELECT c1 FROM t) FROM t", schema,
                quantifyOperatorMatcher(firstOperandMatcher, secondOperandMatcher, 0, 1)::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("args")
    public void greaterThanOrEqual(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        assertPlan("SELECT c1 >= SOME(SELECT c2 FROM t) FROM t", schema,
                quantifyOperatorMatcher(firstOperandMatcher, secondOperandMatcher, 0, 1)::matches, List.of());
        assertPlan("SELECT c2 >= SOME(SELECT c1 FROM t) FROM t", schema,
                quantifyOperatorMatcher(secondOperandMatcher, firstOperandMatcher, 0, 1)::matches, List.of());
        assertPlan("SELECT c1 >= ALL(SELECT c2 FROM t) FROM t", schema,
                quantifyOperatorMatcher(firstOperandMatcher, secondOperandMatcher, 0, 1)::matches, List.of());
        assertPlan("SELECT c2 >= ALL(SELECT c1 FROM t) FROM t", schema,
                quantifyOperatorMatcher(firstOperandMatcher, secondOperandMatcher, 0, 1)::matches, List.of());
    }

    /**
     * This test ensures that {@link #args()} doesn't miss any type pair from {@link NumericPair}.
     */
    @Test
    void argsIncludesAllTypePairs() {
        checkIncludesAllTypePairs(args(), DatetimePair.class);
    }

    private static Matcher<IgniteRel> equalsToSomeOperatorMatcher(
            Matcher<RexNode> first, Matcher<RexNode> second
    ) {
        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object actual) {
                RelNode projectRel = (RelNode) actual;
                Join joinRel = (Join) projectRel.getInput(0);

                RexNode comparison = joinRel.getCondition();

                assertThat(comparison, instanceOf(RexCall.class));

                RexCall comparisonCall = (RexCall) comparison;

                RexNode leftOperand = comparisonCall.getOperands().get(0);
                RexNode rightOperand = comparisonCall.getOperands().get(1);

                assertThat(leftOperand, first);
                assertThat(rightOperand, second);

                return true;
            }

            @Override
            public void describeTo(Description description) {

            }
        };
    }

    private static Matcher<IgniteRel> quantifyOperatorMatcher(
            Matcher<RexNode> first, Matcher<RexNode> second, int firstOpIdx, int secondOpIdx
    ) {
        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object actual) {
                RexNode expression = ((Project) actual).getProjects().get(0);

                List<RexNode> comparisonToVerify = findComparison(expression, firstOpIdx, secondOpIdx);

                assertThat(comparisonToVerify, not(Matchers.empty()));

                for (RexNode comparison : comparisonToVerify) {
                    assertThat(comparison, instanceOf(RexCall.class));

                    RexCall comparisonCall = (RexCall) comparison;

                    RexNode leftOperand = comparisonCall.getOperands().get(0);
                    RexNode rightOperand = comparisonCall.getOperands().get(1);

                    assertThat(leftOperand, first);
                    assertThat(rightOperand, second);
                }

                return true;
            }

            @Override
            public void describeTo(Description description) {

            }
        };
    }

    /** Finds a comparison call comparing input references with provided indexes. */
    private static List<RexNode> findComparison(RexNode expression, int firstOpIdx, int secondOpIdx) {
        List<RexNode> result = new ArrayList<>();
        new RexShuttle() {
            @Override
            public RexNode visitCall(RexCall call) {
                if (call.getKind().belongsTo(SqlKind.BINARY_COMPARISON)) {
                    result.add(call);

                    return call;
                }

                return super.visitCall(call);
            }
        }.apply(expression);

        result.removeIf(cmp -> {
            try {
                new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        int column = inputRef.getIndex();
                        if (column != firstOpIdx && column != secondOpIdx) {
                            throw Util.FoundOne.NULL;
                        }

                        return inputRef;
                    }
                }.apply(cmp);
            } catch (Util.FoundOne ignored) {
                return true;
            }

            return false;
        });

        return result;
    }
}
