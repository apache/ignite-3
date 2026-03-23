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

import static org.apache.ignite.internal.util.CollectionUtils.last;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.DatetimePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of tests to verify behavior of type coercion for UPDATE operations, when values belongs to DATETIME types.
 *
 * <p>This tests aim to help to understand in which cases implicit cast will be added to which values.
 */
public class DateTimeUpdateSourcesCoercionTest extends BaseTypeCoercionTest {

    @ParameterizedTest
    @MethodSource("argsForUpdateWithLiteralValue")
    public void updateWithLiteralValue(
            TypePair pair,
            Matcher<RexNode> operandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.second());

        String val = generateLiteralWithNoRepetition(pair.second());
        assertPlan("UPDATE T SET c1=" + val, schema, modifyOperandMatcher(operandMatcher)::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("argsDyn")
    public void updateDynamicParameters(
            TypePair pair,
            Matcher<RexNode> operandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.second());

        Object val = SqlTestUtils.generateValueByType(pair.second());

        assertPlan("UPDATE T SET c1=?", schema, modifyOperandMatcher(operandMatcher)::matches, List.of(val));
    }

    @ParameterizedTest
    @MethodSource("argsForUpdateWithColumnAsValue")
    public void updateWithColumnAsValue(
            TypePair pair,
            Matcher<RexNode> operandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.second());

        assertPlan("UPDATE T SET C1=C2", schema, modifyOperandMatcher(operandMatcher)::matches, List.of());
    }

    /**
     * This test ensures that {@link #argsForUpdateWithLiteralValue()}, {@link #argsForUpdateWithColumnAsValue()} and {@link #argsDyn()}
     * doesn't miss any type pair from {@link DatetimePair}.
     */
    @Test
    void argsIncludesAllTypePairs() {
        checkIncludesAllTypePairs(argsForUpdateWithLiteralValue(), DatetimePair.class);
        checkIncludesAllTypePairs(argsForUpdateWithColumnAsValue(), DatetimePair.class);
        checkIncludesAllTypePairs(argsDyn(), DatetimePair.class);
    }

    private static Matcher<IgniteRel> modifyOperandMatcher(Matcher<RexNode> matcher) {
        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object actual) {
                RexNode expression = last(((IgniteTableScan) ((IgniteTableModify) actual).getInput()).projects());

                assertThat(expression, matcher);

                return true;
            }

            @Override
            public void describeTo(Description description) {

            }
        };
    }

    private static Stream<Arguments> argsForUpdateWithLiteralValue() {
        return Stream.of(
                forTypePair(DatetimePair.DATE_DATE)
                        .opMatches(ofTypeWithoutCast(NativeTypes.DATE)),
                // TIME

                forTypePair(DatetimePair.TIME_0_TIME_0)
                        .opMatches(ofTypeWithoutCast(Types.TIME_0)),
                forTypePair(DatetimePair.TIME_0_TIME_3)
                        .opMatches(ofTypeWithoutCast(Types.TIME_0)),
                forTypePair(DatetimePair.TIME_0_TIME_6)
                        .opMatches(ofTypeWithoutCast(Types.TIME_0)),
                forTypePair(DatetimePair.TIME_0_TIME_9)
                        .opMatches(ofTypeWithoutCast(Types.TIME_0)),

                forTypePair(DatetimePair.TIME_3_TIME_3)
                        .opMatches(ofTypeWithoutCast(Types.TIME_3)),
                forTypePair(DatetimePair.TIME_3_TIME_6)
                        .opMatches(ofTypeWithoutCast(Types.TIME_3)),
                forTypePair(DatetimePair.TIME_3_TIME_9)
                        .opMatches(ofTypeWithoutCast(Types.TIME_3)),

                forTypePair(DatetimePair.TIME_6_TIME_6)
                        .opMatches(ofTypeWithoutCast(Types.TIME_6)),
                forTypePair(DatetimePair.TIME_6_TIME_9)
                        .opMatches(ofTypeWithoutCast(Types.TIME_6)),

                forTypePair(DatetimePair.TIME_9_TIME_9)
                        .opMatches(ofTypeWithoutCast(Types.TIME_9)),

                // TIMESTAMP 0

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_0)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_0)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_3)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_0)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_6)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_0)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_9)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_0)),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_0)
                        .opMatches(castTo(Types.TIMESTAMP_0)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_3)
                        .opMatches(castTo(Types.TIMESTAMP_0)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_6)
                        .opMatches(castTo(Types.TIMESTAMP_0)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_9)
                        .opMatches(castTo(Types.TIMESTAMP_0)),

                // TIMESTAMP 3

                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_3)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_3)),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_6)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_3)),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_9)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_3)),

                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_3)
                        .opMatches(castTo(Types.TIMESTAMP_3)),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_6)
                        .opMatches(castTo(Types.TIMESTAMP_3)),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_9)
                        .opMatches(castTo(Types.TIMESTAMP_3)),

                // TIMESTAMP 6

                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_6)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_9)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_6)),

                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_WLTZ_6)
                        .opMatches(castTo(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_WLTZ_9)
                        .opMatches(castTo(Types.TIMESTAMP_6)),

                // TIMESTAMP 9

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_9)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_9)
                        .opMatches(castTo(Types.TIMESTAMP_9)),

                // TIMESTAMP LTZ 0

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_0)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_0)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_3)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_0)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_6)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_0)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_9)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_0)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_3)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_6)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0)),

                // TIMESTAMP LTZ 3

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_3)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_3)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_6)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_3)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_9)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_3)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_3)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_3)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_6)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_3)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_9)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_3)),

                // TIMESTAMP LTZ 6

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_6)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_6)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_9)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_6)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_6)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_6)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_9)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_6)),

                // TIMESTAMP LTZ 9

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_9)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_9)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9))
        );
    }

    private static Stream<Arguments> argsForUpdateWithColumnAsValue() {
        Map<DatetimePair, Arguments> diff = new EnumMap<>(DatetimePair.class);

        // TIME

        diff.put(DatetimePair.TIME_0_TIME_3, forTypePair(DatetimePair.TIME_0_TIME_3)
                .opMatches(castTo(Types.TIME_0)));
        diff.put(DatetimePair.TIME_0_TIME_6, forTypePair(DatetimePair.TIME_0_TIME_6)
                .opMatches(castTo(Types.TIME_0)));
        diff.put(DatetimePair.TIME_0_TIME_9, forTypePair(DatetimePair.TIME_0_TIME_9)
                .opMatches(castTo(Types.TIME_0)));

        diff.put(DatetimePair.TIME_3_TIME_6, forTypePair(DatetimePair.TIME_3_TIME_6)
                .opMatches(castTo(Types.TIME_3)));
        diff.put(DatetimePair.TIME_3_TIME_9, forTypePair(DatetimePair.TIME_3_TIME_9)
                .opMatches(castTo(Types.TIME_3)));

        diff.put(DatetimePair.TIME_6_TIME_9, forTypePair(DatetimePair.TIME_6_TIME_9)
                .opMatches(castTo(Types.TIME_6)));

        // TIMESTAMP

        diff.put(DatetimePair.TIMESTAMP_0_TIMESTAMP_3, forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_3)
                .opMatches(castTo(Types.TIMESTAMP_0)));
        diff.put(DatetimePair.TIMESTAMP_0_TIMESTAMP_6, forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_6)
                .opMatches(castTo(Types.TIMESTAMP_0)));
        diff.put(DatetimePair.TIMESTAMP_0_TIMESTAMP_9, forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_9)
                .opMatches(castTo(Types.TIMESTAMP_0)));

        diff.put(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_0, forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_0)
                .opMatches(castTo(Types.TIMESTAMP_0)));
        diff.put(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_3, forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_3)
                .opMatches(castTo(Types.TIMESTAMP_0)));
        diff.put(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_9, forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_9)
                .opMatches(castTo(Types.TIMESTAMP_0)));

        diff.put(DatetimePair.TIMESTAMP_3_TIMESTAMP_6, forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_6)
                .opMatches(castTo(Types.TIMESTAMP_3)));
        diff.put(DatetimePair.TIMESTAMP_3_TIMESTAMP_9, forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_9)
                .opMatches(castTo(Types.TIMESTAMP_3)));

        diff.put(DatetimePair.TIMESTAMP_6_TIMESTAMP_9, forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_9)
                .opMatches(castTo(Types.TIMESTAMP_6)));

        // TIMESTAMP LTZ

        diff.put(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_0, forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_0)
                .opMatches(castTo(Types.TIMESTAMP_WLTZ_0)));
        diff.put(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_3, forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_3)
                .opMatches(castTo(Types.TIMESTAMP_WLTZ_0)));
        diff.put(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_6, forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_6)
                .opMatches(castTo(Types.TIMESTAMP_WLTZ_0)));
        diff.put(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_9, forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_9)
                .opMatches(castTo(Types.TIMESTAMP_WLTZ_0)));

        diff.put(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_3, forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_3)
                .opMatches(castTo(Types.TIMESTAMP_WLTZ_0)));
        diff.put(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_6, forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_6)
                .opMatches(castTo(Types.TIMESTAMP_WLTZ_0)));
        diff.put(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9, forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9)
                .opMatches(castTo(Types.TIMESTAMP_WLTZ_0)));

        diff.put(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_3, forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_3)
                .opMatches(castTo(Types.TIMESTAMP_WLTZ_3)));
        diff.put(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_9, forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_9)
                .opMatches(castTo(Types.TIMESTAMP_WLTZ_3)));

        diff.put(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_6, forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_6)
                .opMatches(castTo(Types.TIMESTAMP_WLTZ_3)));
        diff.put(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_9, forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_9)
                .opMatches(castTo(Types.TIMESTAMP_WLTZ_3)));

        diff.put(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_9, forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_9)
                .opMatches(castTo(Types.TIMESTAMP_WLTZ_6)));

        return argsForUpdateWithLiteralValue().map(v -> diff.getOrDefault(v.get()[0], v));
    }

    private static Stream<Arguments> argsDyn() {
        Map<DatetimePair, Arguments> diff = new EnumMap<>(DatetimePair.class);

        // TIME

        diff.put(DatetimePair.TIME_0_TIME_0, forTypePair(DatetimePair.TIME_0_TIME_0)
                .opMatches(castTo(Types.TIME_0)));

        diff.put(DatetimePair.TIME_0_TIME_3, forTypePair(DatetimePair.TIME_0_TIME_3)
                .opMatches(castTo(Types.TIME_0)));

        diff.put(DatetimePair.TIME_0_TIME_6, forTypePair(DatetimePair.TIME_0_TIME_6)
                .opMatches(castTo(Types.TIME_0)));

        diff.put(DatetimePair.TIME_0_TIME_9, forTypePair(DatetimePair.TIME_0_TIME_9)
                .opMatches(castTo(Types.TIME_0)));

        diff.put(DatetimePair.TIME_3_TIME_3, forTypePair(DatetimePair.TIME_3_TIME_3)
                .opMatches(castTo(Types.TIME_3)));

        diff.put(DatetimePair.TIME_3_TIME_6, forTypePair(DatetimePair.TIME_3_TIME_6)
                .opMatches(castTo(Types.TIME_3)));

        diff.put(DatetimePair.TIME_6_TIME_6, forTypePair(DatetimePair.TIME_6_TIME_6)
                .opMatches(castTo(Types.TIME_6)));

        diff.put(DatetimePair.TIME_6_TIME_9, forTypePair(DatetimePair.TIME_6_TIME_9)
                .opMatches(castTo(Types.TIME_6)));

        diff.put(DatetimePair.TIME_9_TIME_9, forTypePair(DatetimePair.TIME_9_TIME_9)
                .opMatches(ofTypeWithoutCast(Types.TIME_9)));

        // TIMESTAMP

        diff.put(DatetimePair.TIMESTAMP_0_TIMESTAMP_0, forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_0)
                .opMatches(castTo(Types.TIMESTAMP_0)));

        diff.put(DatetimePair.TIMESTAMP_3_TIMESTAMP_3, forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_3)
                .opMatches(castTo(Types.TIMESTAMP_3)));

        diff.put(DatetimePair.TIMESTAMP_6_TIMESTAMP_6, forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_6)
                .opMatches(castTo(Types.TIMESTAMP_6)));

        diff.put(DatetimePair.TIMESTAMP_6_TIMESTAMP_9, forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_9)
                .opMatches(castTo(Types.TIMESTAMP_6)));

        // TIMESTAMP LTZ

        diff.put(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0, forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0)
                .opMatches(castTo(Types.TIMESTAMP_WLTZ_0)));

        diff.put(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_3, forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_3)
                .opMatches(castTo(Types.TIMESTAMP_WLTZ_3)));

        diff.put(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_6, forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_6)
                .opMatches(castTo(Types.TIMESTAMP_WLTZ_6)));

        diff.put(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_9, forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_9)
                .opMatches(castTo(Types.TIMESTAMP_WLTZ_6)));

        return argsForUpdateWithColumnAsValue().map(v -> diff.getOrDefault(v.get()[0], v));
    }
}
