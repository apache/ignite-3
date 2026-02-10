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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.NumericPair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.type.NativeTypes;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of tests to verify behavior of type coercion for binary arithmetic, when operands belongs to the NUMERIC type family.
 *
 * <p>This tests aim to help to understand in which cases implicit cast will be added to which operand when does arithmetic with a given
 * numeric type pair.
 */
public class NumericBinaryOperationsTypeCoercionTest extends BaseTypeCoercionTest {

    // No any type changes for `addition` operation from planner perspective.
    @ParameterizedTest
    @MethodSource("plusMinusArgs")
    public void additionOp(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher,
            Matcher<RexCall> resultsMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        assertPlan("SELECT c1 + c2 FROM t", schema,
                operandsWithResultMatcher(firstOperandMatcher, secondOperandMatcher, resultsMatcher)::matches, List.of());
        assertPlan("SELECT c2 + c1 FROM t", schema,
                operandsWithResultMatcher(secondOperandMatcher, firstOperandMatcher, resultsMatcher)::matches, List.of());
    }

    // No any type changes for `subtraction` operation from planner perspective.
    @ParameterizedTest
    @MethodSource("plusMinusArgs")
    public void subtractionOp(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher,
            Matcher<RexCall> resultsMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        assertPlan("SELECT c1 - c2 FROM t", schema,
                operandsWithResultMatcher(firstOperandMatcher, secondOperandMatcher, resultsMatcher)::matches, List.of());
        assertPlan("SELECT c2 - c1 FROM t", schema,
                operandsWithResultMatcher(secondOperandMatcher, firstOperandMatcher, resultsMatcher)::matches, List.of());
    }

    // No any type changes for `division` operation from planner perspective.
    @ParameterizedTest
    @MethodSource("divArgs")
    public void divisionOp(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher,
            Matcher<RexCall> resultsMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        assertPlan("SELECT c1 / c2 FROM t", schema,
                operandsWithResultMatcher(firstOperandMatcher, secondOperandMatcher, resultsMatcher)::matches, List.of());
        assertPlan("SELECT c2 / c1 FROM t", schema, operandMatcher(secondOperandMatcher, firstOperandMatcher)::matches, List.of());
    }

    // No any type changes for `multiplication` operation from planner perspective.
    @ParameterizedTest
    @MethodSource("multArgs")
    public void multiplicationOp(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher,
            Matcher<RexCall> resultsMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        assertPlan("SELECT c1 * c2 FROM t", schema,
                operandsWithResultMatcher(firstOperandMatcher, secondOperandMatcher, resultsMatcher)::matches, List.of());
        assertPlan("SELECT c2 * c1 FROM t", schema,
                operandsWithResultMatcher(secondOperandMatcher, firstOperandMatcher, resultsMatcher)::matches, List.of());
    }

    // Have the following casts for modulo operation:
    // REAL datatype always casts to DECIMAL(14,7)
    // DOUBLE datatype always casts to DECIMAL(30,15)
    // Any other types with no any changes
    @ParameterizedTest
    @MethodSource("moduloArgs")
    public void moduloOp(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher,
            Matcher<RexCall> resultsMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        assertPlan("SELECT c1 % c2 FROM t", schema,
                operandsWithResultMatcher(firstOperandMatcher, secondOperandMatcher, resultsMatcher)::matches, List.of());
        assertPlan("SELECT c2 % c1 FROM t", schema,
                operandMatcher(secondOperandMatcher, firstOperandMatcher)::matches, List.of());
    }

    /**
     * This test ensures that {@link #moduloArgs()} doesn't miss any type pair from {@link NumericPair}.
     */
    @Test
    void checkAllTypePairs() {
        checkIncludesAllNumericTypePairs(moduloArgs());
        checkIncludesAllNumericTypePairs(plusMinusArgs());
        checkIncludesAllNumericTypePairs(multArgs());
        checkIncludesAllNumericTypePairs(divArgs());
    }

    private static Stream<Arguments> moduloArgs() {
        return Stream.of(
                forTypePairEx(NumericPair.TINYINT_TINYINT)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(NativeTypes.INT8),

                forTypePairEx(NumericPair.TINYINT_SMALLINT)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(NativeTypes.INT16),

                forTypePairEx(NumericPair.TINYINT_INT)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(NativeTypes.INT32),

                forTypePairEx(NumericPair.TINYINT_BIGINT)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(NativeTypes.INT64),

                forTypePairEx(NumericPair.TINYINT_DECIMAL_1_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_1_0),

                forTypePairEx(NumericPair.TINYINT_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.TINYINT_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.TINYINT_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_0),

                forTypePairEx(NumericPair.TINYINT_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_3_1),

                forTypePairEx(NumericPair.TINYINT_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.TINYINT_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_0),

                forTypePairEx(NumericPair.TINYINT_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_1),

                forTypePairEx(NumericPair.TINYINT_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_6_3),

                forTypePairEx(NumericPair.TINYINT_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_10_7),

                forTypePairEx(NumericPair.TINYINT_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_18_15),

                forTypePairEx(NumericPair.SMALLINT_SMALLINT)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(NativeTypes.INT16),

                forTypePairEx(NumericPair.SMALLINT_INT)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(NativeTypes.INT32),

                forTypePairEx(NumericPair.SMALLINT_BIGINT)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(NativeTypes.INT64),

                forTypePairEx(NumericPair.SMALLINT_DECIMAL_1_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_1_0),

                forTypePairEx(NumericPair.SMALLINT_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.SMALLINT_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.SMALLINT_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_0),

                forTypePairEx(NumericPair.SMALLINT_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_3_1),

                forTypePairEx(NumericPair.SMALLINT_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.SMALLINT_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_0),

                forTypePairEx(NumericPair.SMALLINT_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_6_1),

                forTypePairEx(NumericPair.SMALLINT_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_8_3),

                forTypePairEx(NumericPair.SMALLINT_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_12_7),

                forTypePairEx(NumericPair.SMALLINT_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_20_15),

                forTypePairEx(NumericPair.INT_INT)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(NativeTypes.INT32),

                forTypePairEx(NumericPair.INT_BIGINT)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(NativeTypes.INT64),

                forTypePairEx(NumericPair.INT_DECIMAL_1_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_1_0),

                forTypePairEx(NumericPair.INT_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.INT_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.INT_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_0),

                forTypePairEx(NumericPair.INT_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_3_1),

                forTypePairEx(NumericPair.INT_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.INT_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_0),

                forTypePairEx(NumericPair.INT_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_6_1),

                forTypePairEx(NumericPair.INT_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_8_3),

                forTypePairEx(NumericPair.INT_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_14_7),

                forTypePairEx(NumericPair.INT_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_25_15),

                forTypePairEx(NumericPair.BIGINT_BIGINT)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(NativeTypes.INT64),

                forTypePairEx(NumericPair.BIGINT_DECIMAL_1_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_1_0),

                forTypePairEx(NumericPair.BIGINT_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.BIGINT_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.BIGINT_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_0),

                forTypePairEx(NumericPair.BIGINT_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_3_1),

                forTypePairEx(NumericPair.BIGINT_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.BIGINT_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_0),

                forTypePairEx(NumericPair.BIGINT_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_6_1),

                forTypePairEx(NumericPair.BIGINT_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_8_3),

                forTypePairEx(NumericPair.BIGINT_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_14_7),

                forTypePairEx(NumericPair.BIGINT_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_30_15),

                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_1_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_1_0),

                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_0),

                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_0),

                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_1_0_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_8_7),

                forTypePairEx(NumericPair.DECIMAL_1_0_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_16_15),

                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_2_1_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_8_7),

                forTypePairEx(NumericPair.DECIMAL_2_1_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_16_15),

                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_4_3_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_8_7),

                forTypePairEx(NumericPair.DECIMAL_4_3_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_16_15),

                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_0),

                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_3_1),

                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_0),

                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_3_1),

                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.DECIMAL_2_0_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_9_7),

                forTypePairEx(NumericPair.DECIMAL_2_0_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_17_15),

                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_3_1),

                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_3_1),

                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_3_1),

                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.DECIMAL_3_1_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_9_7),

                forTypePairEx(NumericPair.DECIMAL_3_1_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_17_15),

                forTypePairEx(NumericPair.DECIMAL_5_3_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.DECIMAL_5_3_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.DECIMAL_5_3_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.DECIMAL_5_3_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.DECIMAL_5_3_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_9_7),

                forTypePairEx(NumericPair.DECIMAL_5_3_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_17_15),

                forTypePairEx(NumericPair.DECIMAL_5_0_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_0),

                forTypePairEx(NumericPair.DECIMAL_5_0_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_6_1),

                forTypePairEx(NumericPair.DECIMAL_5_0_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_8_3),

                forTypePairEx(NumericPair.DECIMAL_5_0_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_12_7),

                forTypePairEx(NumericPair.DECIMAL_5_0_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_20_15),

                forTypePairEx(NumericPair.DECIMAL_6_1_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_6_1),

                forTypePairEx(NumericPair.DECIMAL_6_1_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_8_3),

                forTypePairEx(NumericPair.DECIMAL_6_1_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_12_7),

                forTypePairEx(NumericPair.DECIMAL_6_1_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_20_15),

                forTypePairEx(NumericPair.DECIMAL_8_3_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_8_3),

                forTypePairEx(NumericPair.DECIMAL_8_3_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_12_7),

                forTypePairEx(NumericPair.DECIMAL_8_3_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_20_15),

                forTypePairEx(NumericPair.REAL_REAL)
                        .firstOpMatches(castTo(Types.DECIMAL_14_7))
                        .secondOpMatches(castTo((Types.DECIMAL_14_7)))
                        .resultWillBe(Types.DECIMAL_14_7),

                forTypePairEx(NumericPair.REAL_DOUBLE)
                        .firstOpMatches(castTo(Types.DECIMAL_14_7))
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_22_15),

                forTypePairEx(NumericPair.DOUBLE_DOUBLE)
                        .firstOpMatches(castTo(Types.DECIMAL_30_15))
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_30_15)
        );
    }

    private static Stream<Arguments> plusMinusArgs() {
        return Stream.of(
                forTypePairEx(NumericPair.TINYINT_TINYINT).checkResult(NativeTypes.INT8),
                forTypePairEx(NumericPair.TINYINT_SMALLINT).checkResult(NativeTypes.INT16),
                forTypePairEx(NumericPair.TINYINT_INT).checkResult(NativeTypes.INT32),
                forTypePairEx(NumericPair.TINYINT_BIGINT).checkResult(NativeTypes.INT64),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_1_0).checkResult(Types.DECIMAL_4_0),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_2_1).checkResult(Types.DECIMAL_5_1),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_4_3).checkResult(Types.DECIMAL_7_3),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_2_0).checkResult(Types.DECIMAL_4_0),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_3_1).checkResult(Types.DECIMAL_5_1),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_5_3).checkResult(Types.DECIMAL_7_3),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_5_0).checkResult(Types.DECIMAL_6_0),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_6_1).checkResult(Types.DECIMAL_7_1),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_8_3).checkResult(Types.DECIMAL_9_3),
                forTypePairEx(NumericPair.TINYINT_REAL).checkResult(NativeTypes.FLOAT),
                forTypePairEx(NumericPair.TINYINT_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.SMALLINT_SMALLINT).checkResult(NativeTypes.INT16),
                forTypePairEx(NumericPair.SMALLINT_INT).checkResult(NativeTypes.INT32),
                forTypePairEx(NumericPair.SMALLINT_BIGINT).checkResult(NativeTypes.INT64),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_1_0).checkResult(Types.DECIMAL_6_0),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_2_1).checkResult(Types.DECIMAL_7_1),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_4_3).checkResult(Types.DECIMAL_9_3),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_2_0).checkResult(Types.DECIMAL_6_0),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_3_1).checkResult(Types.DECIMAL_7_1),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_5_3).checkResult(Types.DECIMAL_9_3),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_5_0).checkResult(Types.DECIMAL_6_0),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_6_1).checkResult(Types.DECIMAL_7_1),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_8_3).checkResult(Types.DECIMAL_9_3),
                forTypePairEx(NumericPair.SMALLINT_REAL).checkResult(NativeTypes.FLOAT),
                forTypePairEx(NumericPair.SMALLINT_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.INT_INT).checkResult(NativeTypes.INT32),
                forTypePairEx(NumericPair.INT_BIGINT).checkResult(NativeTypes.INT64),
                forTypePairEx(NumericPair.INT_DECIMAL_1_0).checkResult(Types.DECIMAL_11_0),
                forTypePairEx(NumericPair.INT_DECIMAL_2_1).checkResult(Types.DECIMAL_12_1),
                forTypePairEx(NumericPair.INT_DECIMAL_4_3).checkResult(Types.DECIMAL_14_3),
                forTypePairEx(NumericPair.INT_DECIMAL_2_0).checkResult(Types.DECIMAL_11_0),
                forTypePairEx(NumericPair.INT_DECIMAL_3_1).checkResult(Types.DECIMAL_12_1),
                forTypePairEx(NumericPair.INT_DECIMAL_5_3).checkResult(Types.DECIMAL_14_3),
                forTypePairEx(NumericPair.INT_DECIMAL_5_0).checkResult(Types.DECIMAL_11_0),
                forTypePairEx(NumericPair.INT_DECIMAL_6_1).checkResult(Types.DECIMAL_12_1),
                forTypePairEx(NumericPair.INT_DECIMAL_8_3).checkResult(Types.DECIMAL_14_3),
                forTypePairEx(NumericPair.INT_REAL).checkResult(NativeTypes.FLOAT),
                forTypePairEx(NumericPair.INT_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.BIGINT_BIGINT).checkResult(NativeTypes.INT64),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_1_0).checkResult(Types.DECIMAL_20_0),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_2_1).checkResult(Types.DECIMAL_21_1),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_4_3).checkResult(Types.DECIMAL_23_3),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_2_0).checkResult(Types.DECIMAL_20_0),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_3_1).checkResult(Types.DECIMAL_21_1),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_5_3).checkResult(Types.DECIMAL_23_3),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_5_0).checkResult(Types.DECIMAL_20_0),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_6_1).checkResult(Types.DECIMAL_21_1),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_8_3).checkResult(Types.DECIMAL_23_3),
                forTypePairEx(NumericPair.BIGINT_REAL).checkResult(NativeTypes.FLOAT),
                forTypePairEx(NumericPair.BIGINT_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_1_0).checkResult(Types.DECIMAL_2_0),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_2_1).checkResult(Types.DECIMAL_3_1),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_4_3).checkResult(Types.DECIMAL_5_3),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_2_0).checkResult(Types.DECIMAL_3_0),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_3_1).checkResult(Types.DECIMAL_4_1),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_5_3).checkResult(Types.DECIMAL_6_3),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_5_0).checkResult(Types.DECIMAL_6_0),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_6_1).checkResult(Types.DECIMAL_7_1),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_8_3).checkResult(Types.DECIMAL_9_3),
                forTypePairEx(NumericPair.DECIMAL_1_0_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_1_0_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_2_1).checkResult(Types.DECIMAL_3_1),
                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_4_3).checkResult(Types.DECIMAL_5_3),
                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_2_0).checkResult(Types.DECIMAL_4_1),
                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_3_1).checkResult(Types.DECIMAL_4_1),
                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_5_3).checkResult(Types.DECIMAL_6_3),
                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_5_0).checkResult(Types.DECIMAL_7_1),
                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_6_1).checkResult(Types.DECIMAL_7_1),
                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_8_3).checkResult(Types.DECIMAL_9_3),
                forTypePairEx(NumericPair.DECIMAL_2_1_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_2_1_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_4_3).checkResult(Types.DECIMAL_5_3),
                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_2_0).checkResult(Types.DECIMAL_6_3),
                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_3_1).checkResult(Types.DECIMAL_6_3),
                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_5_3).checkResult(Types.DECIMAL_6_3),
                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_5_0).checkResult(Types.DECIMAL_9_3),
                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_6_1).checkResult(Types.DECIMAL_9_3),
                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_8_3).checkResult(Types.DECIMAL_9_3),
                forTypePairEx(NumericPair.DECIMAL_4_3_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_4_3_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_2_0).checkResult(Types.DECIMAL_3_0),
                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_3_1).checkResult(Types.DECIMAL_4_1),
                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_5_3).checkResult(Types.DECIMAL_6_3),
                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_5_0).checkResult(Types.DECIMAL_6_0),
                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_6_1).checkResult(Types.DECIMAL_7_1),
                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_8_3).checkResult(Types.DECIMAL_9_3),
                forTypePairEx(NumericPair.DECIMAL_2_0_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_2_0_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_3_1).checkResult(Types.DECIMAL_4_1),
                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_5_3).checkResult(Types.DECIMAL_6_3),
                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_5_0).checkResult(Types.DECIMAL_7_1),
                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_6_1).checkResult(Types.DECIMAL_7_1),
                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_8_3).checkResult(Types.DECIMAL_9_3),
                forTypePairEx(NumericPair.DECIMAL_3_1_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_3_1_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_5_3_DECIMAL_5_3).checkResult(Types.DECIMAL_6_3),
                forTypePairEx(NumericPair.DECIMAL_5_3_DECIMAL_5_0).checkResult(Types.DECIMAL_9_3),
                forTypePairEx(NumericPair.DECIMAL_5_3_DECIMAL_6_1).checkResult(Types.DECIMAL_9_3),
                forTypePairEx(NumericPair.DECIMAL_5_3_DECIMAL_8_3).checkResult(Types.DECIMAL_9_3),
                forTypePairEx(NumericPair.DECIMAL_5_3_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_5_3_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_5_0_DECIMAL_5_0).checkResult(Types.DECIMAL_6_0),
                forTypePairEx(NumericPair.DECIMAL_5_0_DECIMAL_6_1).checkResult(Types.DECIMAL_7_1),
                forTypePairEx(NumericPair.DECIMAL_5_0_DECIMAL_8_3).checkResult(Types.DECIMAL_9_3),
                forTypePairEx(NumericPair.DECIMAL_5_0_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_5_0_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_6_1_DECIMAL_6_1).checkResult(Types.DECIMAL_7_1),
                forTypePairEx(NumericPair.DECIMAL_6_1_DECIMAL_8_3).checkResult(Types.DECIMAL_9_3),
                forTypePairEx(NumericPair.DECIMAL_6_1_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_6_1_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_8_3_DECIMAL_8_3).checkResult(Types.DECIMAL_9_3),
                forTypePairEx(NumericPair.DECIMAL_8_3_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_8_3_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.REAL_REAL).checkResult(NativeTypes.FLOAT),
                forTypePairEx(NumericPair.REAL_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DOUBLE_DOUBLE).checkResult(NativeTypes.DOUBLE)
        );
    }

    private static Stream<Arguments> multArgs() {
        return Stream.of(
                forTypePairEx(NumericPair.TINYINT_TINYINT).checkResult(NativeTypes.INT8),
                forTypePairEx(NumericPair.TINYINT_SMALLINT).checkResult(NativeTypes.INT16),
                forTypePairEx(NumericPair.TINYINT_INT).checkResult(NativeTypes.INT32),
                forTypePairEx(NumericPair.TINYINT_BIGINT).checkResult(NativeTypes.INT64),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_1_0).checkResult(Types.DECIMAL_4_0),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_2_1).checkResult(Types.DECIMAL_5_1),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_4_3).checkResult(Types.DECIMAL_7_3),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_2_0).checkResult(Types.DECIMAL_5_0),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_3_1).checkResult(Types.DECIMAL_6_1),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_5_3).checkResult(Types.DECIMAL_8_3),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_5_0).checkResult(Types.DECIMAL_8_0),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_6_1).checkResult(Types.DECIMAL_9_1),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_8_3).checkResult(Types.DECIMAL_11_3),
                forTypePairEx(NumericPair.TINYINT_REAL).checkResult(NativeTypes.FLOAT),
                forTypePairEx(NumericPair.TINYINT_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.SMALLINT_SMALLINT).checkResult(NativeTypes.INT16),
                forTypePairEx(NumericPair.SMALLINT_INT).checkResult(NativeTypes.INT32),
                forTypePairEx(NumericPair.SMALLINT_BIGINT).checkResult(NativeTypes.INT64),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_1_0).checkResult(Types.DECIMAL_6_0),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_2_1).checkResult(Types.DECIMAL_7_1),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_4_3).checkResult(Types.DECIMAL_9_3),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_2_0).checkResult(Types.DECIMAL_7_0),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_3_1).checkResult(Types.DECIMAL_8_1),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_5_3).checkResult(Types.DECIMAL_10_3),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_5_0).checkResult(Types.DECIMAL_10_0),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_6_1).checkResult(Types.DECIMAL_11_1),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_8_3).checkResult(Types.DECIMAL_13_3),
                forTypePairEx(NumericPair.SMALLINT_REAL).checkResult(NativeTypes.FLOAT),
                forTypePairEx(NumericPair.SMALLINT_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.INT_INT).checkResult(NativeTypes.INT32),
                forTypePairEx(NumericPair.INT_BIGINT).checkResult(NativeTypes.INT64),
                forTypePairEx(NumericPair.INT_DECIMAL_1_0).checkResult(Types.DECIMAL_11_0),
                forTypePairEx(NumericPair.INT_DECIMAL_2_1).checkResult(Types.DECIMAL_12_1),
                forTypePairEx(NumericPair.INT_DECIMAL_4_3).checkResult(Types.DECIMAL_14_3),
                forTypePairEx(NumericPair.INT_DECIMAL_2_0).checkResult(Types.DECIMAL_12_0),
                forTypePairEx(NumericPair.INT_DECIMAL_3_1).checkResult(Types.DECIMAL_13_1),
                forTypePairEx(NumericPair.INT_DECIMAL_5_3).checkResult(Types.DECIMAL_15_3),
                forTypePairEx(NumericPair.INT_DECIMAL_5_0).checkResult(Types.DECIMAL_15_0),
                forTypePairEx(NumericPair.INT_DECIMAL_6_1).checkResult(Types.DECIMAL_16_1),
                forTypePairEx(NumericPair.INT_DECIMAL_8_3).checkResult(Types.DECIMAL_18_3),
                forTypePairEx(NumericPair.INT_REAL).checkResult(NativeTypes.FLOAT),
                forTypePairEx(NumericPair.INT_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.BIGINT_BIGINT).checkResult(NativeTypes.INT64),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_1_0).checkResult(Types.DECIMAL_20_0),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_2_1).checkResult(Types.DECIMAL_21_1),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_4_3).checkResult(Types.DECIMAL_23_3),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_2_0).checkResult(Types.DECIMAL_21_0),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_3_1).checkResult(Types.DECIMAL_22_1),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_5_3).checkResult(Types.DECIMAL_24_3),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_5_0).checkResult(Types.DECIMAL_24_0),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_6_1).checkResult(Types.DECIMAL_25_1),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_8_3).checkResult(Types.DECIMAL_27_3),
                forTypePairEx(NumericPair.BIGINT_REAL).checkResult(NativeTypes.FLOAT),
                forTypePairEx(NumericPair.BIGINT_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_1_0).checkResult(Types.DECIMAL_2_0),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_2_1).checkResult(Types.DECIMAL_3_1),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_4_3).checkResult(Types.DECIMAL_5_3),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_2_0).checkResult(Types.DECIMAL_3_0),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_3_1).checkResult(Types.DECIMAL_4_1),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_5_3).checkResult(Types.DECIMAL_6_3),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_5_0).checkResult(Types.DECIMAL_6_0),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_6_1).checkResult(Types.DECIMAL_7_1),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_8_3).checkResult(Types.DECIMAL_9_3),
                forTypePairEx(NumericPair.DECIMAL_1_0_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_1_0_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_2_1).checkResult(Types.DECIMAL_4_2),
                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_4_3).checkResult(Types.DECIMAL_6_4),
                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_2_0).checkResult(Types.DECIMAL_4_1),
                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_3_1).checkResult(Types.DECIMAL_5_2),
                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_5_3).checkResult(Types.DECIMAL_7_4),
                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_5_0).checkResult(Types.DECIMAL_7_1),
                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_6_1).checkResult(Types.DECIMAL_8_2),
                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_8_3).checkResult(Types.DECIMAL_10_4),
                forTypePairEx(NumericPair.DECIMAL_2_1_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_2_1_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_4_3).checkResult(Types.DECIMAL_8_6),
                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_2_0).checkResult(Types.DECIMAL_6_3),
                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_3_1).checkResult(Types.DECIMAL_7_4),
                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_5_3).checkResult(Types.DECIMAL_9_6),
                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_5_0).checkResult(Types.DECIMAL_9_3),
                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_6_1).checkResult(Types.DECIMAL_10_4),
                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_8_3).checkResult(Types.DECIMAL_12_6),
                forTypePairEx(NumericPair.DECIMAL_4_3_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_4_3_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_2_0).checkResult(Types.DECIMAL_4_0),
                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_3_1).checkResult(Types.DECIMAL_5_1),
                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_5_3).checkResult(Types.DECIMAL_7_3),
                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_5_0).checkResult(Types.DECIMAL_7_0),
                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_6_1).checkResult(Types.DECIMAL_8_1),
                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_8_3).checkResult(Types.DECIMAL_10_3),
                forTypePairEx(NumericPair.DECIMAL_2_0_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_2_0_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_3_1).checkResult(Types.DECIMAL_6_2),
                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_5_3).checkResult(Types.DECIMAL_8_4),
                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_5_0).checkResult(Types.DECIMAL_8_1),
                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_6_1).checkResult(Types.DECIMAL_9_2),
                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_8_3).checkResult(Types.DECIMAL_11_4),
                forTypePairEx(NumericPair.DECIMAL_3_1_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_3_1_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_5_3_DECIMAL_5_3).checkResult(Types.DECIMAL_10_6),
                forTypePairEx(NumericPair.DECIMAL_5_3_DECIMAL_5_0).checkResult(Types.DECIMAL_10_3),
                forTypePairEx(NumericPair.DECIMAL_5_3_DECIMAL_6_1).checkResult(Types.DECIMAL_11_4),
                forTypePairEx(NumericPair.DECIMAL_5_3_DECIMAL_8_3).checkResult(Types.DECIMAL_13_6),
                forTypePairEx(NumericPair.DECIMAL_5_3_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_5_3_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_5_0_DECIMAL_5_0).checkResult(Types.DECIMAL_10_0),
                forTypePairEx(NumericPair.DECIMAL_5_0_DECIMAL_6_1).checkResult(Types.DECIMAL_11_1),
                forTypePairEx(NumericPair.DECIMAL_5_0_DECIMAL_8_3).checkResult(Types.DECIMAL_13_3),
                forTypePairEx(NumericPair.DECIMAL_5_0_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_5_0_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_6_1_DECIMAL_6_1).checkResult(Types.DECIMAL_12_2),
                forTypePairEx(NumericPair.DECIMAL_6_1_DECIMAL_8_3).checkResult(Types.DECIMAL_14_4),
                forTypePairEx(NumericPair.DECIMAL_6_1_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_6_1_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_8_3_DECIMAL_8_3).checkResult(Types.DECIMAL_16_6),
                forTypePairEx(NumericPair.DECIMAL_8_3_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_8_3_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.REAL_REAL).checkResult(NativeTypes.FLOAT),
                forTypePairEx(NumericPair.REAL_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DOUBLE_DOUBLE).checkResult(NativeTypes.DOUBLE)
        );
    }

    private static Stream<Arguments> divArgs() {
        return Stream.of(
                forTypePairEx(NumericPair.TINYINT_TINYINT).checkResult(NativeTypes.INT8),
                forTypePairEx(NumericPair.TINYINT_SMALLINT).checkResult(NativeTypes.INT16),
                forTypePairEx(NumericPair.TINYINT_INT).checkResult(NativeTypes.INT32),
                forTypePairEx(NumericPair.TINYINT_BIGINT).checkResult(NativeTypes.INT64),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_1_0).checkResult(Types.DECIMAL_9_6),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_2_1).checkResult(Types.DECIMAL_10_6),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_4_3).checkResult(Types.DECIMAL_12_6),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_2_0).checkResult(Types.DECIMAL_9_6),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_3_1).checkResult(Types.DECIMAL_10_6),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_5_3).checkResult(Types.DECIMAL_12_6),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_5_0).checkResult(Types.DECIMAL_9_6),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_6_1).checkResult(Types.DECIMAL_11_7),
                forTypePairEx(NumericPair.TINYINT_DECIMAL_8_3).checkResult(Types.DECIMAL_15_9),
                forTypePairEx(NumericPair.TINYINT_REAL).checkResult(NativeTypes.FLOAT),
                forTypePairEx(NumericPair.TINYINT_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.SMALLINT_SMALLINT).checkResult(NativeTypes.INT16),
                forTypePairEx(NumericPair.SMALLINT_INT).checkResult(NativeTypes.INT32),
                forTypePairEx(NumericPair.SMALLINT_BIGINT).checkResult(NativeTypes.INT64),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_1_0).checkResult(Types.DECIMAL_11_6),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_2_1).checkResult(Types.DECIMAL_12_6),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_4_3).checkResult(Types.DECIMAL_14_6),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_2_0).checkResult(Types.DECIMAL_11_6),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_3_1).checkResult(Types.DECIMAL_12_6),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_5_3).checkResult(Types.DECIMAL_14_6),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_5_0).checkResult(Types.DECIMAL_11_6),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_6_1).checkResult(Types.DECIMAL_13_7),
                forTypePairEx(NumericPair.SMALLINT_DECIMAL_8_3).checkResult(Types.DECIMAL_17_9),
                forTypePairEx(NumericPair.SMALLINT_REAL).checkResult(NativeTypes.FLOAT),
                forTypePairEx(NumericPair.SMALLINT_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.INT_INT).checkResult(NativeTypes.INT32),
                forTypePairEx(NumericPair.INT_BIGINT).checkResult(NativeTypes.INT64),
                forTypePairEx(NumericPair.INT_DECIMAL_1_0).checkResult(Types.DECIMAL_16_6),
                forTypePairEx(NumericPair.INT_DECIMAL_2_1).checkResult(Types.DECIMAL_17_6),
                forTypePairEx(NumericPair.INT_DECIMAL_4_3).checkResult(Types.DECIMAL_19_6),
                forTypePairEx(NumericPair.INT_DECIMAL_2_0).checkResult(Types.DECIMAL_16_6),
                forTypePairEx(NumericPair.INT_DECIMAL_3_1).checkResult(Types.DECIMAL_17_6),
                forTypePairEx(NumericPair.INT_DECIMAL_5_3).checkResult(Types.DECIMAL_19_6),
                forTypePairEx(NumericPair.INT_DECIMAL_5_0).checkResult(Types.DECIMAL_16_6),
                forTypePairEx(NumericPair.INT_DECIMAL_6_1).checkResult(Types.DECIMAL_18_7),
                forTypePairEx(NumericPair.INT_DECIMAL_8_3).checkResult(Types.DECIMAL_22_9),
                forTypePairEx(NumericPair.INT_REAL).checkResult(NativeTypes.FLOAT),
                forTypePairEx(NumericPair.INT_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.BIGINT_BIGINT).checkResult(NativeTypes.INT64),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_1_0).checkResult(Types.DECIMAL_25_6),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_2_1).checkResult(Types.DECIMAL_26_6),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_4_3).checkResult(Types.DECIMAL_28_6),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_2_0).checkResult(Types.DECIMAL_25_6),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_3_1).checkResult(Types.DECIMAL_26_6),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_5_3).checkResult(Types.DECIMAL_28_6),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_5_0).checkResult(Types.DECIMAL_25_6),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_6_1).checkResult(Types.DECIMAL_27_7),
                forTypePairEx(NumericPair.BIGINT_DECIMAL_8_3).checkResult(Types.DECIMAL_31_9),
                forTypePairEx(NumericPair.BIGINT_REAL).checkResult(NativeTypes.FLOAT),
                forTypePairEx(NumericPair.BIGINT_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_1_0).checkResult(Types.DECIMAL_7_6),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_2_1).checkResult(Types.DECIMAL_8_6),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_4_3).checkResult(Types.DECIMAL_10_6),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_2_0).checkResult(Types.DECIMAL_7_6),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_3_1).checkResult(Types.DECIMAL_8_6),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_5_3).checkResult(Types.DECIMAL_10_6),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_5_0).checkResult(Types.DECIMAL_7_6),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_6_1).checkResult(Types.DECIMAL_9_7),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_8_3).checkResult(Types.DECIMAL_13_9),
                forTypePairEx(NumericPair.DECIMAL_1_0_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_1_0_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_2_1).checkResult(Types.DECIMAL_8_6),
                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_4_3).checkResult(Types.DECIMAL_10_6),
                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_2_0).checkResult(Types.DECIMAL_7_6),
                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_3_1).checkResult(Types.DECIMAL_8_6),
                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_5_3).checkResult(Types.DECIMAL_11_7),
                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_5_0).checkResult(Types.DECIMAL_8_7),
                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_6_1).checkResult(Types.DECIMAL_10_8),
                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_8_3).checkResult(Types.DECIMAL_14_10),
                forTypePairEx(NumericPair.DECIMAL_2_1_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_2_1_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_4_3).checkResult(Types.DECIMAL_12_8),
                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_2_0).checkResult(Types.DECIMAL_7_6),
                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_3_1).checkResult(Types.DECIMAL_9_7),
                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_5_3).checkResult(Types.DECIMAL_13_9),
                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_5_0).checkResult(Types.DECIMAL_10_9),
                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_6_1).checkResult(Types.DECIMAL_12_10),
                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_8_3).checkResult(Types.DECIMAL_16_12),
                forTypePairEx(NumericPair.DECIMAL_4_3_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_4_3_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_2_0).checkResult(Types.DECIMAL_8_6),
                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_3_1).checkResult(Types.DECIMAL_9_6),
                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_5_3).checkResult(Types.DECIMAL_11_6),
                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_5_0).checkResult(Types.DECIMAL_8_6),
                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_6_1).checkResult(Types.DECIMAL_10_7),
                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_8_3).checkResult(Types.DECIMAL_14_9),
                forTypePairEx(NumericPair.DECIMAL_2_0_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_2_0_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_3_1).checkResult(Types.DECIMAL_9_6),
                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_5_3).checkResult(Types.DECIMAL_12_7),
                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_5_0).checkResult(Types.DECIMAL_9_7),
                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_6_1).checkResult(Types.DECIMAL_11_8),
                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_8_3).checkResult(Types.DECIMAL_15_10),
                forTypePairEx(NumericPair.DECIMAL_3_1_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_3_1_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_5_3_DECIMAL_5_3).checkResult(Types.DECIMAL_14_9),
                forTypePairEx(NumericPair.DECIMAL_5_3_DECIMAL_5_0).checkResult(Types.DECIMAL_11_9),
                forTypePairEx(NumericPair.DECIMAL_5_3_DECIMAL_6_1).checkResult(Types.DECIMAL_13_10),
                forTypePairEx(NumericPair.DECIMAL_5_3_DECIMAL_8_3).checkResult(Types.DECIMAL_17_12),
                forTypePairEx(NumericPair.DECIMAL_5_3_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_5_3_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_5_0_DECIMAL_5_0).checkResult(Types.DECIMAL_11_6),
                forTypePairEx(NumericPair.DECIMAL_5_0_DECIMAL_6_1).checkResult(Types.DECIMAL_13_7),
                forTypePairEx(NumericPair.DECIMAL_5_0_DECIMAL_8_3).checkResult(Types.DECIMAL_17_9),
                forTypePairEx(NumericPair.DECIMAL_5_0_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_5_0_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_6_1_DECIMAL_6_1).checkResult(Types.DECIMAL_14_8),
                forTypePairEx(NumericPair.DECIMAL_6_1_DECIMAL_8_3).checkResult(Types.DECIMAL_18_10),
                forTypePairEx(NumericPair.DECIMAL_6_1_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_6_1_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DECIMAL_8_3_DECIMAL_8_3).checkResult(Types.DECIMAL_20_12),
                forTypePairEx(NumericPair.DECIMAL_8_3_REAL).checkResult(NativeTypes.DOUBLE),
                forTypePairEx(NumericPair.DECIMAL_8_3_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.REAL_REAL).checkResult(NativeTypes.FLOAT),
                forTypePairEx(NumericPair.REAL_DOUBLE).checkResult(NativeTypes.DOUBLE),

                forTypePairEx(NumericPair.DOUBLE_DOUBLE).checkResult(NativeTypes.DOUBLE)
        );
    }
}
