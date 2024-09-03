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

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.NumericPair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.type.NativeTypes;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of test to verify behavior of type coercion for INSERT operations, when values belongs to the NUMERIC type family.
 *
 * <p>This tests aim to help to understand in which cases implicit cast will be added to which values.
 */
public class NumericInsertSourcesCoercionTest extends BaseTypeCoercionTest {

    @ParameterizedTest
    @MethodSource("args")
    public void insert(
            TypePair pair,
            Matcher<RexNode> operandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.first());

        // SHORT values can intersect with a DECIMAL with a 5 digits in integer parts, so for SHORT (INT16) we need to generate values
        // take it into consideration.
        boolean closerToBound = pair.first().spec() == NativeTypeSpec.INT16;

        String value = generateLiteral(pair.second(), closerToBound);
        assertPlan("INSERT INTO T VALUES(" + value + "," + value + ")", schema, keyValOperandMatcher(operandMatcher)::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("argsDyn")
    public void insertDynamicParameters(
            TypePair pair,
            Matcher<RexNode> operandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.first());

        Object val = SqlTestUtils.generateValueByType(pair.second());
        assertPlan("INSERT INTO T VALUES(?, ?)", schema, keyValOperandMatcher(operandMatcher)::matches, List.of(val, val));
    }


    /**
     * This test ensures that {@link #args()} and {@link #argsDyn()} doesn't miss any type pair from {@link NumericPair}.
     */
    @Test
    void insertArgsIncludesAllTypePairs() {
        checkIncludesAllNumericTypePairs(args());
        checkIncludesAllNumericTypePairs(argsDyn());
    }

    private static Matcher<IgniteRel> keyValOperandMatcher(Matcher<RexNode> matcher) {
        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object actual) {
                List<RexNode> expressions = ((IgniteKeyValueModify) actual).expressions();

                RexNode leftOperand = expressions.get(0);
                RexNode rightOperand = expressions.get(1);

                assertThat(leftOperand, matcher);
                assertThat(rightOperand, matcher);

                return true;
            }

            @Override
            public void describeTo(Description description) {

            }
        };
    }

    private static Stream<Arguments> args() {
        return Stream.of(
                forTypePair(NumericPair.TINYINT_TINYINT)
                        .opMatches(ofTypeWithoutCast(NativeTypes.INT8)),

                forTypePair(NumericPair.TINYINT_SMALLINT)
                        .opMatches(castTo(NativeTypes.INT8)),

                forTypePair(NumericPair.TINYINT_INT)
                        .opMatches(castTo(NativeTypes.INT8)),

                forTypePair(NumericPair.TINYINT_BIGINT)
                        .opMatches(castTo(NativeTypes.INT8)),

                forTypePair(NumericPair.TINYINT_DECIMAL_1_0)
                        .opMatches(ofTypeWithoutCast(NativeTypes.INT8)),

                forTypePair(NumericPair.TINYINT_DECIMAL_2_1)
                        .opMatches(castTo(NativeTypes.INT8)),

                forTypePair(NumericPair.TINYINT_DECIMAL_4_3)
                        .opMatches(castTo(NativeTypes.INT8)),

                forTypePair(NumericPair.TINYINT_DECIMAL_2_0)
                        .opMatches(ofTypeWithoutCast(NativeTypes.INT8)),

                forTypePair(NumericPair.TINYINT_DECIMAL_3_1)
                        .opMatches(castTo(NativeTypes.INT8)),

                forTypePair(NumericPair.TINYINT_DECIMAL_5_3)
                        .opMatches(castTo(NativeTypes.INT8)),

                forTypePair(NumericPair.TINYINT_DECIMAL_5_0)
                        .opMatches(castTo(NativeTypes.INT8)),

                forTypePair(NumericPair.TINYINT_DECIMAL_6_1)
                        .opMatches(castTo(NativeTypes.INT8)),

                forTypePair(NumericPair.TINYINT_DECIMAL_8_3)
                        .opMatches(castTo(NativeTypes.INT8)),

                forTypePair(NumericPair.TINYINT_REAL)
                        .opMatches(castTo(NativeTypes.INT8)),

                forTypePair(NumericPair.TINYINT_DOUBLE)
                        .opMatches(castTo(NativeTypes.INT8)),

                forTypePair(NumericPair.SMALLINT_SMALLINT)
                        .opMatches(ofTypeWithoutCast(NativeTypes.INT16)),

                forTypePair(NumericPair.SMALLINT_INT)
                        .opMatches(castTo(NativeTypes.INT16)),

                forTypePair(NumericPair.SMALLINT_BIGINT)
                        .opMatches(castTo(NativeTypes.INT16)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_1_0)
                        .opMatches(ofTypeWithoutCast(NativeTypes.INT16)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_2_1)
                        .opMatches(castTo(NativeTypes.INT16)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_4_3)
                        .opMatches(castTo(NativeTypes.INT16)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_2_0)
                        .opMatches(ofTypeWithoutCast(NativeTypes.INT16)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_3_1)
                        .opMatches(castTo(NativeTypes.INT16)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_5_3)
                        .opMatches(castTo(NativeTypes.INT16)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_5_0)
                        .opMatches(castTo(NativeTypes.INT16)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_6_1)
                        .opMatches(castTo(NativeTypes.INT16)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_8_3)
                        .opMatches(castTo(NativeTypes.INT16)),

                forTypePair(NumericPair.SMALLINT_REAL)
                        .opMatches(castTo(NativeTypes.INT16)),

                forTypePair(NumericPair.SMALLINT_DOUBLE)
                        .opMatches(castTo(NativeTypes.INT16)),

                forTypePair(NumericPair.INT_INT)
                        .opMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.INT_BIGINT)
                        .opMatches(castTo(NativeTypes.INT32)),

                forTypePair(NumericPair.INT_DECIMAL_1_0)
                        .opMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.INT_DECIMAL_2_1)
                        .opMatches(castTo(NativeTypes.INT32)),

                forTypePair(NumericPair.INT_DECIMAL_4_3)
                        .opMatches(castTo(NativeTypes.INT32)),

                forTypePair(NumericPair.INT_DECIMAL_2_0)
                        .opMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.INT_DECIMAL_3_1)
                        .opMatches(castTo(NativeTypes.INT32)),

                forTypePair(NumericPair.INT_DECIMAL_5_3)
                        .opMatches(castTo(NativeTypes.INT32)),

                forTypePair(NumericPair.INT_DECIMAL_5_0)
                        .opMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.INT_DECIMAL_6_1)
                        .opMatches(castTo(NativeTypes.INT32)),

                forTypePair(NumericPair.INT_DECIMAL_8_3)
                        .opMatches(castTo(NativeTypes.INT32)),

                forTypePair(NumericPair.INT_REAL)
                        .opMatches(castTo(NativeTypes.INT32)),

                forTypePair(NumericPair.INT_DOUBLE)
                        .opMatches(castTo(NativeTypes.INT32)),

                forTypePair(NumericPair.BIGINT_BIGINT)
                        .opMatches(ofTypeWithoutCast(NativeTypes.INT64)),

                forTypePair(NumericPair.BIGINT_DECIMAL_1_0)
                        .opMatches(ofTypeWithoutCast(NativeTypes.INT64)),

                forTypePair(NumericPair.BIGINT_DECIMAL_2_1)
                        .opMatches(castTo(NativeTypes.INT64)),

                forTypePair(NumericPair.BIGINT_DECIMAL_4_3)
                        .opMatches(castTo(NativeTypes.INT64)),

                forTypePair(NumericPair.BIGINT_DECIMAL_2_0)
                        .opMatches(ofTypeWithoutCast(NativeTypes.INT64)),

                forTypePair(NumericPair.BIGINT_DECIMAL_3_1)
                        .opMatches(castTo(NativeTypes.INT64)),

                forTypePair(NumericPair.BIGINT_DECIMAL_5_3)
                        .opMatches(castTo(NativeTypes.INT64)),

                forTypePair(NumericPair.BIGINT_DECIMAL_5_0)
                        .opMatches(ofTypeWithoutCast(NativeTypes.INT64)),

                forTypePair(NumericPair.BIGINT_DECIMAL_6_1)
                        .opMatches(castTo(NativeTypes.INT64)),

                forTypePair(NumericPair.BIGINT_DECIMAL_8_3)
                        .opMatches(castTo(NativeTypes.INT64)),

                forTypePair(NumericPair.BIGINT_REAL)
                        .opMatches(castTo(NativeTypes.INT64)),

                forTypePair(NumericPair.BIGINT_DOUBLE)
                        .opMatches(castTo(NativeTypes.INT64)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_1_0)
                        .opMatches(ofTypeWithoutCast(Types.DECIMAL_1_0)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_1)
                        .opMatches(ofTypeWithoutCast(Types.DECIMAL_1_0)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_4_3)
                        .opMatches(ofTypeWithoutCast(Types.DECIMAL_1_0)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_0)
                        .opMatches(castTo(Types.DECIMAL_1_0)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_3_1)
                        .opMatches(castTo(Types.DECIMAL_1_0)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_3)
                        .opMatches(castTo(Types.DECIMAL_1_0)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_0)
                        .opMatches(castTo(Types.DECIMAL_1_0)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_6_1)
                        .opMatches(castTo(Types.DECIMAL_1_0)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_8_3)
                        .opMatches(castTo(Types.DECIMAL_1_0)),

                forTypePair(NumericPair.DECIMAL_1_0_REAL)
                        .opMatches(castTo(Types.DECIMAL_1_0)),

                forTypePair(NumericPair.DECIMAL_1_0_DOUBLE)
                        .opMatches(castTo(Types.DECIMAL_1_0)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_1)
                        .opMatches(ofTypeWithoutCast(Types.DECIMAL_2_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_4_3)
                        .opMatches(ofTypeWithoutCast(Types.DECIMAL_2_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_0)
                        .opMatches(castTo(Types.DECIMAL_2_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_3_1)
                        .opMatches(castTo(Types.DECIMAL_2_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_3)
                        .opMatches(castTo(Types.DECIMAL_2_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_0)
                        .opMatches(castTo(Types.DECIMAL_2_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_6_1)
                        .opMatches(castTo(Types.DECIMAL_2_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_8_3)
                        .opMatches(castTo(Types.DECIMAL_2_1)),

                forTypePair(NumericPair.DECIMAL_2_1_REAL)
                        .opMatches(castTo(Types.DECIMAL_2_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DOUBLE)
                        .opMatches(castTo(Types.DECIMAL_2_1)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_4_3)
                        .opMatches(ofTypeWithoutCast(Types.DECIMAL_4_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_2_0)
                        .opMatches(castTo(Types.DECIMAL_4_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_3_1)
                        .opMatches(castTo(Types.DECIMAL_4_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_3)
                        .opMatches(castTo(Types.DECIMAL_4_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_0)
                        .opMatches(castTo(Types.DECIMAL_4_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_6_1)
                        .opMatches(castTo(Types.DECIMAL_4_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_8_3)
                        .opMatches(castTo(Types.DECIMAL_4_3)),

                forTypePair(NumericPair.DECIMAL_4_3_REAL)
                        .opMatches(castTo(Types.DECIMAL_4_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DOUBLE)
                        .opMatches(castTo(Types.DECIMAL_4_3)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_2_0)
                        .opMatches(ofTypeWithoutCast(Types.DECIMAL_2_0)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_3_1)
                        .opMatches(ofTypeWithoutCast(Types.DECIMAL_2_0)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_3)
                        .opMatches(ofTypeWithoutCast(Types.DECIMAL_2_0)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_0)
                        .opMatches(castTo(Types.DECIMAL_2_0)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_6_1)
                        .opMatches(castTo(Types.DECIMAL_2_0)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_8_3)
                        .opMatches(castTo(Types.DECIMAL_2_0)),

                forTypePair(NumericPair.DECIMAL_2_0_REAL)
                        .opMatches(castTo(Types.DECIMAL_2_0)),

                forTypePair(NumericPair.DECIMAL_2_0_DOUBLE)
                        .opMatches(castTo(Types.DECIMAL_2_0)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_3_1)
                        .opMatches(ofTypeWithoutCast(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_3)
                        .opMatches(ofTypeWithoutCast(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_0)
                        .opMatches(castTo(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_6_1)
                        .opMatches(castTo(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_8_3)
                        .opMatches(castTo(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.DECIMAL_3_1_REAL)
                        .opMatches(castTo(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.DECIMAL_3_1_DOUBLE)
                        .opMatches(castTo(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_3)
                        .opMatches(ofTypeWithoutCast(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_0)
                        .opMatches(castTo(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_6_1)
                        .opMatches(castTo(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_8_3)
                        .opMatches(castTo(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_5_3_REAL)
                        .opMatches(castTo(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_5_3_DOUBLE)
                        .opMatches(castTo(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_5_0)
                        .opMatches(ofTypeWithoutCast(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_6_1)
                        .opMatches(ofTypeWithoutCast(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_8_3)
                        .opMatches(ofTypeWithoutCast(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.DECIMAL_5_0_REAL)
                        .opMatches(castTo(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.DECIMAL_5_0_DOUBLE)
                        .opMatches(castTo(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_6_1)
                        .opMatches(ofTypeWithoutCast(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_8_3)
                        .opMatches(ofTypeWithoutCast(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_6_1_REAL)
                        .opMatches(castTo(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_6_1_DOUBLE)
                        .opMatches(castTo(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_8_3_DECIMAL_8_3)
                        .opMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_8_3_REAL)
                        .opMatches(castTo(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_8_3_DOUBLE)
                        .opMatches(castTo(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.REAL_REAL)
                        .opMatches(ofTypeWithoutCast(NativeTypes.FLOAT)),

                forTypePair(NumericPair.REAL_DOUBLE)
                        .opMatches(ofTypeWithoutCast(NativeTypes.FLOAT)),

                forTypePair(NumericPair.DOUBLE_DOUBLE)
                        .opMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
        );
    }

    private static Stream<Arguments> argsDyn() {
        // Difference between the original parameters.
        Map<NumericPair, Arguments> map = new EnumMap<>(NumericPair.class);
        map.put(NumericPair.REAL_DOUBLE, forTypePair(NumericPair.REAL_DOUBLE).opMatches(castTo(NativeTypes.FLOAT)));
        map.put(NumericPair.TINYINT_DECIMAL_1_0, forTypePair(NumericPair.TINYINT_DECIMAL_1_0).opMatches(castTo(NativeTypes.INT8)));
        map.put(NumericPair.TINYINT_DECIMAL_2_0, forTypePair(NumericPair.TINYINT_DECIMAL_2_0).opMatches(castTo(NativeTypes.INT8)));
        map.put(NumericPair.INT_DECIMAL_1_0, forTypePair(NumericPair.INT_DECIMAL_1_0).opMatches(castTo(NativeTypes.INT32)));
        map.put(NumericPair.INT_DECIMAL_2_0, forTypePair(NumericPair.INT_DECIMAL_2_0).opMatches(castTo(NativeTypes.INT32)));
        map.put(NumericPair.INT_DECIMAL_5_0, forTypePair(NumericPair.INT_DECIMAL_5_0).opMatches(castTo(NativeTypes.INT32)));
        map.put(NumericPair.SMALLINT_DECIMAL_1_0, forTypePair(NumericPair.SMALLINT_DECIMAL_1_0).opMatches(castTo(NativeTypes.INT16)));
        map.put(NumericPair.SMALLINT_DECIMAL_2_0, forTypePair(NumericPair.SMALLINT_DECIMAL_2_0).opMatches(castTo(NativeTypes.INT16)));
        map.put(NumericPair.BIGINT_DECIMAL_1_0, forTypePair(NumericPair.BIGINT_DECIMAL_1_0).opMatches(castTo(NativeTypes.INT64)));
        map.put(NumericPair.BIGINT_DECIMAL_2_0, forTypePair(NumericPair.BIGINT_DECIMAL_2_0).opMatches(castTo(NativeTypes.INT64)));
        map.put(NumericPair.BIGINT_DECIMAL_5_0, forTypePair(NumericPair.BIGINT_DECIMAL_5_0).opMatches(castTo(NativeTypes.INT64)));
        map.put(NumericPair.DECIMAL_1_0_DECIMAL_1_0,
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_1_0).opMatches(castTo((Types.DECIMAL_1_0))));
        map.put(NumericPair.DECIMAL_1_0_DECIMAL_2_1,
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_1).opMatches(castTo((Types.DECIMAL_1_0))));
        map.put(NumericPair.DECIMAL_1_0_DECIMAL_4_3,
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_4_3).opMatches(castTo((Types.DECIMAL_1_0))));
        map.put(NumericPair.DECIMAL_2_1_DECIMAL_2_1,
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_1).opMatches(castTo((Types.DECIMAL_2_1))));
        map.put(NumericPair.DECIMAL_2_1_DECIMAL_4_3,
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_4_3).opMatches(castTo((Types.DECIMAL_2_1))));
        map.put(NumericPair.DECIMAL_4_3_DECIMAL_4_3,
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_4_3).opMatches(castTo((Types.DECIMAL_4_3))));
        map.put(NumericPair.DECIMAL_2_0_DECIMAL_2_0,
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_2_0).opMatches(castTo((Types.DECIMAL_2_0))));
        map.put(NumericPair.DECIMAL_2_0_DECIMAL_3_1,
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_3_1).opMatches(castTo((Types.DECIMAL_2_0))));
        map.put(NumericPair.DECIMAL_2_0_DECIMAL_5_3,
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_3).opMatches(castTo((Types.DECIMAL_2_0))));
        map.put(NumericPair.DECIMAL_3_1_DECIMAL_3_1,
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_3_1).opMatches(castTo((Types.DECIMAL_3_1))));
        map.put(NumericPair.DECIMAL_3_1_DECIMAL_5_3,
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_3).opMatches(castTo((Types.DECIMAL_3_1))));
        map.put(NumericPair.DECIMAL_5_3_DECIMAL_5_3,
                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_3).opMatches(castTo((Types.DECIMAL_5_3))));
        map.put(NumericPair.DECIMAL_5_0_DECIMAL_5_0,
                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_5_0).opMatches(castTo((Types.DECIMAL_5_0))));
        map.put(NumericPair.DECIMAL_5_0_DECIMAL_6_1,
                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_6_1).opMatches(castTo((Types.DECIMAL_5_0))));
        map.put(NumericPair.DECIMAL_5_0_DECIMAL_8_3,
                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_8_3).opMatches(castTo((Types.DECIMAL_5_0))));
        map.put(NumericPair.DECIMAL_6_1_DECIMAL_6_1,
                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_6_1).opMatches(castTo((Types.DECIMAL_6_1))));
        map.put(NumericPair.DECIMAL_6_1_DECIMAL_8_3,
                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_8_3).opMatches(castTo((Types.DECIMAL_6_1))));
        map.put(NumericPair.DECIMAL_8_3_DECIMAL_8_3,
                forTypePair(NumericPair.DECIMAL_8_3_DECIMAL_8_3).opMatches(castTo((Types.DECIMAL_8_3))));

        return args().map(v -> map.getOrDefault(v.get()[0], v));
    }
}
