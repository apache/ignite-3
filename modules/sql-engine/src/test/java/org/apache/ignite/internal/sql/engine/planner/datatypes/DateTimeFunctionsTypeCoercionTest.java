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

import static org.hamcrest.CoreMatchers.any;

import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.sql.ColumnType;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of tests to verify behavior of type coercion for numeric functions, when operand belong to DATETIME types.
 */
public class DateTimeFunctionsTypeCoercionTest extends BaseTypeCoercionTest {

    @ParameterizedTest
    @MethodSource("dateTimeTypes")
    public void floor(NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(nativeType), any(RexNode.class));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(nativeType);

        assertPlan("SELECT FLOOR(C1 TO SECOND) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("dateTimeTypes")
    public void ceil(NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(nativeType), any(RexNode.class));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(nativeType);

        assertPlan("SELECT CEIL(C1 TO SECOND) FROM T", schema, matcher::matches, List.of());
    }

    private static Stream<NativeType> dateTimeTypes() {
        return Stream.of(
                Types.TIME_0,
                Types.TIME_3,
                Types.TIME_6,
                Types.TIME_9,
                Types.TIMESTAMP_0,
                Types.TIMESTAMP_3,
                Types.TIMESTAMP_6,
                Types.TIMESTAMP_9,
                Types.TIMESTAMP_WLTZ_0,
                Types.TIMESTAMP_WLTZ_3,
                Types.TIMESTAMP_WLTZ_6,
                Types.TIMESTAMP_WLTZ_9
        );
    }

    @Test
    public void lastDay() throws Exception {
        NativeType nativeType = NativeTypes.DATE;
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(nativeType));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DATE);

        assertPlan("SELECT LAST_DAY(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("dateTimestamps")
    public void dayName(NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(nativeType));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args)
                .returnTypeNullability(false)
                .resultWillBe(NativeTypes.stringOf(2000));

        assertPlan("SELECT DAYNAME(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("dateTimestamps")
    public void monthName(NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(nativeType));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args)
                .returnTypeNullability(false)
                .resultWillBe(NativeTypes.stringOf(2000));

        assertPlan("SELECT MONTHNAME(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("dateTimestamps")
    public void dayOfMonth(NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(any(RexNode.class), ofTypeWithoutCast(nativeType));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.INT64);

        assertPlan("SELECT DAYOFMONTH(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("dateTimestamps")
    public void dayOfWeek(NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(any(RexNode.class), ofTypeWithoutCast(nativeType));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.INT64);

        assertPlan("SELECT DAYOFWEEK(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("dateTimestamps")
    public void dayOfYear(NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(any(RexNode.class), ofTypeWithoutCast(nativeType));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.INT64);

        assertPlan("SELECT DAYOFYEAR(C1) FROM T", schema, matcher::matches, List.of());
    }

    private static Stream<NativeType> dateTimestamps() {
        return Stream.of(
                Types.DATE,
                Types.TIMESTAMP_0,
                Types.TIMESTAMP_3,
                Types.TIMESTAMP_6,
                Types.TIMESTAMP_9,
                Types.TIMESTAMP_WLTZ_0,
                Types.TIMESTAMP_WLTZ_3,
                Types.TIMESTAMP_WLTZ_6,
                Types.TIMESTAMP_WLTZ_9
        );
    }

    @ParameterizedTest
    @MethodSource("extractTime")
    public void extractTime(String field, NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(any(RexNode.class), ofTypeWithoutCast(nativeType));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.INT64);

        assertPlan("SELECT EXTRACT(" + field + " FROM C1) FROM T", schema, matcher::matches, List.of());
        assertPlan("SELECT " + field + " (C1) FROM T", schema, matcher::matches, List.of());
    }

    private static Stream<Arguments> extractTime() {
        List<NativeType> types = List.of(
                Types.TIME_0,
                Types.TIME_3,
                Types.TIME_6,
                Types.TIME_9
        );

        return Stream.of("HOUR", "MINUTE", "SECOND").flatMap(f -> types.stream().map(t -> Arguments.of(f, t)));
    }

    @ParameterizedTest
    @MethodSource("extractDate")
    public void extractDate(String field, NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(any(RexNode.class), ofTypeWithoutCast(nativeType));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.INT64);

        assertPlan("SELECT EXTRACT(" + field + " FROM C1) FROM T", schema, matcher::matches, List.of());
        assertPlan("SELECT " + field + " (C1) FROM T", schema, matcher::matches, List.of());
    }

    private static Stream<Arguments> extractDate() {
        return Stream.of("YEAR", "QUARTER", "MONTH", "WEEK").map(f -> Arguments.of(f, Types.DATE));
    }

    @ParameterizedTest
    @MethodSource("extractTimestamp")
    public void extractTimestamp(String field, NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(any(RexNode.class), ofTypeWithoutCast(nativeType));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.INT64);

        assertPlan("SELECT EXTRACT(" + field + " FROM C1) FROM T", schema, matcher::matches, List.of());
        assertPlan("SELECT " + field + " (C1) FROM T", schema, matcher::matches, List.of());
    }

    private static Stream<Arguments> extractTimestamp() {
        List<NativeType> types = List.of(
                Types.TIMESTAMP_0,
                Types.TIMESTAMP_3,
                Types.TIMESTAMP_6,
                Types.TIMESTAMP_9,
                Types.TIMESTAMP_WLTZ_0,
                Types.TIMESTAMP_WLTZ_3,
                Types.TIMESTAMP_WLTZ_3,
                Types.TIMESTAMP_WLTZ_9
        );

        return Stream.of(
                "YEAR", "QUARTER", "MONTH", "WEEK",
                "HOUR", "MINUTE", "SECOND"
        ).flatMap(f -> types.stream().map(t -> Arguments.of(f, t)));
    }

    @ParameterizedTest
    @MethodSource("timestamps")
    public void dateFromTimestamp(NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);

        if (nativeType.spec() == ColumnType.DATETIME) {
            List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(nativeType));
            Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DATE);

            assertPlan("SELECT DATE(C1) FROM T", schema, matcher::matches, List.of());
        } else {
            List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(nativeType), any(RexNode.class));
            Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DATE);

            assertPlan("SELECT DATE(C1, 'UTC') FROM T", schema, matcher::matches, List.of());
        }
    }

    private static Stream<NativeType> timestamps() {
        return Stream.of(
                Types.TIMESTAMP_0,
                Types.TIMESTAMP_3,
                Types.TIMESTAMP_6,
                Types.TIMESTAMP_9,
                Types.TIMESTAMP_WLTZ_0,
                Types.TIMESTAMP_WLTZ_3,
                Types.TIMESTAMP_WLTZ_6,
                Types.TIMESTAMP_WLTZ_9
        );
    }
}
