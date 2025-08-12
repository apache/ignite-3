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
 * A set of tests to verify behavior of type coercion for Set operations (UNION, INTERSECT and EXCEPT), when values
 * belong to DATETIME types.
 *
 * <p>This tests aim to help to understand in which cases implicit cast will be added to which values.
 */
public class DateTimeSetOperatorCoercionTest extends BaseTypeCoercionTest {

    @ParameterizedTest
    @MethodSource("args")
    public void unionOperator(
            TypePair pair,
            Matcher<Object> firstOperandMatcher,
            Matcher<Object> secondOperandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoSingleColumnTable(pair.first(), pair.second());

        assertPlan("SELECT c1 FROM t1 UNION ALL SELECT c2 from t2", schema,
                setOperandsMatcher(firstOperandMatcher, secondOperandMatcher)::matches, List.of());
        assertPlan("SELECT c2 FROM t2 UNION ALL SELECT c1 from t1", schema,
                setOperandsMatcher(secondOperandMatcher, firstOperandMatcher)::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("args")
    public void exceptOperator(
            TypePair pair,
            Matcher<Object> firstOperandMatcher,
            Matcher<Object> secondOperandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoSingleColumnTable(pair.first(), pair.second());

        assertPlan("SELECT c1 FROM t1 EXCEPT SELECT c2 from t2", schema,
                setOperandsMatcher(firstOperandMatcher, secondOperandMatcher)::matches, List.of());
        assertPlan("SELECT c2 FROM t2 EXCEPT SELECT c1 from t1", schema,
                setOperandsMatcher(secondOperandMatcher, firstOperandMatcher)::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("args")
    public void intersectOperator(
            TypePair pair,
            Matcher<Object> firstOperandMatcher,
            Matcher<Object> secondOperandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoSingleColumnTable(pair.first(), pair.second());

        assertPlan("SELECT c1 FROM t1 INTERSECT SELECT c2 from t2", schema,
                setOperandsMatcher(firstOperandMatcher, secondOperandMatcher)::matches, List.of());
        assertPlan("SELECT c2 FROM t2 INTERSECT SELECT c1 from t1", schema,
                setOperandsMatcher(secondOperandMatcher, firstOperandMatcher)::matches, List.of());
    }

    private static Stream<Arguments> args() {
        return Stream.of(
                forTypePair(DatetimePair.DATE_DATE)
                        .firstOpMatches(ofJustType(Types.DATE))
                        .secondOpMatches(ofJustType(Types.DATE)),

                // TIME 0

                forTypePair(DatetimePair.TIME_0_TIME_0)
                        .firstOpMatches(ofJustType(Types.TIME_0))
                        .secondOpMatches(ofJustType(Types.TIME_0)),
                forTypePair(DatetimePair.TIME_0_TIME_3)
                        .firstOpMatches(castTo(Types.TIME_3))
                        .secondOpMatches(ofJustType(Types.TIME_3)),
                forTypePair(DatetimePair.TIME_0_TIME_6)
                        .firstOpMatches(castTo(Types.TIME_6))
                        .secondOpMatches(ofJustType(Types.TIME_6)),
                forTypePair(DatetimePair.TIME_0_TIME_9)
                        .firstOpMatches(castTo(Types.TIME_9))
                        .secondOpMatches(ofJustType(Types.TIME_9)),

                // TIME 3

                forTypePair(DatetimePair.TIME_3_TIME_3)
                        .firstOpMatches(ofJustType(Types.TIME_3))
                        .secondOpMatches(ofJustType(Types.TIME_3)),
                forTypePair(DatetimePair.TIME_3_TIME_6)
                        .firstOpMatches(castTo(Types.TIME_6))
                        .secondOpMatches(ofJustType(Types.TIME_6)),
                forTypePair(DatetimePair.TIME_3_TIME_9)
                        .firstOpMatches(castTo(Types.TIME_9))
                        .secondOpMatches(ofJustType(Types.TIME_9)),

                // TIME 6

                forTypePair(DatetimePair.TIME_6_TIME_6)
                        .firstOpMatches(ofJustType(Types.TIME_6))
                        .secondOpMatches(ofJustType(Types.TIME_6)),
                forTypePair(DatetimePair.TIME_6_TIME_9)
                        .firstOpMatches(castTo(Types.TIME_9))
                        .secondOpMatches(ofJustType(Types.TIME_9)),

                // TIME 9

                forTypePair(DatetimePair.TIME_9_TIME_9)
                        .firstOpMatches(ofJustType(Types.TIME_9))
                        .secondOpMatches(ofJustType(Types.TIME_9)),

                // TIMESTAMP 0

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_0)
                        .firstOpMatches(ofJustType(Types.TIMESTAMP_0))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_0)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_3)
                        .firstOpMatches(castTo(Types.TIMESTAMP_3))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_3)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_6))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_0)
                        .firstOpMatches(ofJustType(Types.TIMESTAMP_0))
                        .secondOpMatches(castTo(Types.TIMESTAMP_0)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_3)
                        .firstOpMatches(castTo(Types.TIMESTAMP_3))
                        .secondOpMatches(castTo(Types.TIMESTAMP_3)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_6))
                        .secondOpMatches(castTo(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                // TIMESTAMP 3

                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_3)
                        .firstOpMatches(ofJustType(Types.TIMESTAMP_3))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_3)),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_6))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_3)
                        .firstOpMatches(ofJustType(Types.TIMESTAMP_3))
                        .secondOpMatches(castTo(Types.TIMESTAMP_3)),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_6))
                        .secondOpMatches(castTo(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                // TIMESTAMP 6

                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_6)
                        .firstOpMatches(ofJustType(Types.TIMESTAMP_6))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(ofJustType(Types.TIMESTAMP_6))
                        .secondOpMatches(castTo(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                // TIMESTAMP 9

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_9)
                        .firstOpMatches(ofJustType(Types.TIMESTAMP_9))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofJustType(Types.TIMESTAMP_9))
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                // TIMESTAMP LTZ 0

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0)
                        .firstOpMatches(ofJustType(Types.TIMESTAMP_WLTZ_0))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_WLTZ_0)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_3)
                        .firstOpMatches(castTo(Types.TIMESTAMP_WLTZ_3))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_WLTZ_3)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_WLTZ_6))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_WLTZ_6)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_WLTZ_9))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_WLTZ_9)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_0)
                        .firstOpMatches(castTo(Types.TIMESTAMP_0))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_0)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_3)
                        .firstOpMatches(castTo(Types.TIMESTAMP_3))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_3)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_6))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_9)),

                // TIMESTAMP LTZ 3

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_3)
                        .firstOpMatches(ofJustType(Types.TIMESTAMP_WLTZ_3))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_WLTZ_3)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_WLTZ_6))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_WLTZ_6)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_WLTZ_9))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_WLTZ_9)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_3)
                        .firstOpMatches(castTo(Types.TIMESTAMP_3))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_3)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_6))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_9)),

                // TIMESTAMP LTZ 6

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(ofJustType(Types.TIMESTAMP_WLTZ_6))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_WLTZ_6)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_WLTZ_9))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_WLTZ_9)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_6))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_9)),

                // TIMESTAMP LTZ 9

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofJustType(Types.TIMESTAMP_WLTZ_9))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_WLTZ_9)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpMatches(ofJustType(Types.TIMESTAMP_9))
        );
    }

    /**
     * This test ensures that {@link #args()} doesn't miss any type pair from {@link DatetimePair}.
     */
    @Test
    void argsIncludesAllTypePairs() {
        checkIncludesAllTypePairs(args(), DatetimePair.class);
    }
}
