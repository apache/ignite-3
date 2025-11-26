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

import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.DatetimePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.hamcrest.Matcher;
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
                forTypePair(DatetimePair.TIME_0_TIME_3)
                        .firstOpMatches(ofType(Types.TIME_3))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIME_0_TIME_6)
                        .firstOpMatches(ofType(Types.TIME_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIME_0_TIME_9)
                        .firstOpMatches(ofType(Types.TIME_9))
                        .secondOpBeSame(),

                // TIME 3

                forTypePair(DatetimePair.TIME_3_TIME_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIME_3_TIME_6)
                        .firstOpMatches(ofType(Types.TIME_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIME_3_TIME_9)
                        .firstOpMatches(ofType(Types.TIME_9))
                        .secondOpBeSame(),

                // TIME 6

                forTypePair(DatetimePair.TIME_6_TIME_6)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIME_6_TIME_9)
                        .firstOpMatches(ofType(Types.TIME_9))
                        .secondOpBeSame(),

                // TIME 9

                forTypePair(DatetimePair.TIME_9_TIME_9)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                // TIMESTAMP 0

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_3)
                        .firstOpMatches(ofType(Types.TIMESTAMP_3))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_6)
                        .firstOpMatches(ofType(Types.TIMESTAMP_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_9)
                        .firstOpMatches(ofType(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_0)
                        .firstOpMatches(ofType(Types.TIMESTAMP_0))
                        .secondOpMatches(ofType(Types.TIMESTAMP_0)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_3)
                        .firstOpMatches(ofType(Types.TIMESTAMP_3))
                        .secondOpMatches(ofType(Types.TIMESTAMP_3)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(ofType(Types.TIMESTAMP_6))
                        .secondOpMatches(ofType(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofType(Types.TIMESTAMP_9))
                        .secondOpMatches(ofType(Types.TIMESTAMP_9)),

                // TIMESTAMP 3

                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_6)
                        .firstOpMatches(ofType(Types.TIMESTAMP_6))
                        .secondOpMatches(ofType(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_9)
                        .firstOpMatches(ofType(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_3)
                        .firstOpMatches(ofType(Types.TIMESTAMP_3))
                        .secondOpMatches(ofType(Types.TIMESTAMP_3)),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(ofType(Types.TIMESTAMP_6))
                        .secondOpMatches(ofType(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofType(Types.TIMESTAMP_9))
                        .secondOpMatches(ofType(Types.TIMESTAMP_9)),

                // TIMESTAMP 6

                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_6)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_9)
                        .firstOpMatches(ofType(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(ofType(Types.TIMESTAMP_6))
                        .secondOpMatches(ofType(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofType(Types.TIMESTAMP_9))
                        .secondOpMatches(ofType(Types.TIMESTAMP_9)),

                // TIMESTAMP 9

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_9)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofType(Types.TIMESTAMP_9))
                        .secondOpMatches(ofType(Types.TIMESTAMP_9)),

                // TIMESTAMP LTZ 0

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_3)
                        .firstOpMatches(ofType(Types.TIMESTAMP_WLTZ_3))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(ofType(Types.TIMESTAMP_WLTZ_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofType(Types.TIMESTAMP_WLTZ_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_0)
                        .firstOpMatches(ofType(Types.TIMESTAMP_0))
                        .secondOpMatches(ofType(Types.TIMESTAMP_0)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_3)
                        .firstOpMatches(ofType(Types.TIMESTAMP_3))
                        .secondOpMatches(ofType(Types.TIMESTAMP_3)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_6)
                        .firstOpMatches(ofType(Types.TIMESTAMP_6))
                        .secondOpMatches(ofType(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_9)
                        .firstOpMatches(ofType(Types.TIMESTAMP_9))
                        .secondOpMatches(ofType(Types.TIMESTAMP_9)),

                // TIMESTAMP LTZ 3

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(ofType(Types.TIMESTAMP_WLTZ_6))
                        .secondOpMatches(ofType(Types.TIMESTAMP_WLTZ_6)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofType(Types.TIMESTAMP_WLTZ_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_3)
                        .firstOpMatches(ofType(Types.TIMESTAMP_3))
                        .secondOpMatches(ofType(Types.TIMESTAMP_3)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_6)
                        .firstOpMatches(ofType(Types.TIMESTAMP_6))
                        .secondOpMatches(ofType(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_9)
                        .firstOpMatches(ofType(Types.TIMESTAMP_9))
                        .secondOpMatches(ofType(Types.TIMESTAMP_9)),

                // TIMESTAMP LTZ 6

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_6)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofType(Types.TIMESTAMP_WLTZ_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_6)
                        .firstOpMatches(ofType(Types.TIMESTAMP_6))
                        .secondOpMatches(ofType(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_9)
                        .firstOpMatches(ofType(Types.TIMESTAMP_9))
                        .secondOpMatches(ofType(Types.TIMESTAMP_9)),

                // TIMESTAMP LTZ 9

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_9)
                        .firstOpMatches(ofType(Types.TIMESTAMP_9))
                        .secondOpMatches(ofType(Types.TIMESTAMP_9))
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
     * This test ensures that {@link #args()} doesn't miss any type pair from {@link DatetimePair}.
     */
    @Test
    void argsIncludesAllTypePairs() {
        checkIncludesAllTypePairs(args(), DatetimePair.class);
    }
}
