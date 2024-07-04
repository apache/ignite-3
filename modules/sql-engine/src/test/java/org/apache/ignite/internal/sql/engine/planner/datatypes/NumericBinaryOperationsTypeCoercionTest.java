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
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.NumericPair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of test to verify behavior of type coercion for binary arithmetic, when operands belongs to the NUMERIC type family.
 *
 * <p>This tests aim to help to understand in which cases implicit cast will be added to which operand when does arithmetic with a given
 * numeric type pair.
 */
public class NumericBinaryOperationsTypeCoercionTest extends BaseTypeCoercionTest {

    // No any type changes for `addition` operation from planner perspective.
    @ParameterizedTest
    @MethodSource("allNumericPairs")
    public void additionOp(NumericPair pair) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.second());

        Matcher<RexNode> first = ofTypeWithoutCast(pair.first());
        Matcher<RexNode> second = ofTypeWithoutCast(pair.second());

        assertPlan("SELECT c1 + c2 FROM t", schema, operandMatcher(first, second)::matches, List.of());
        assertPlan("SELECT c2 + c1 FROM t", schema, operandMatcher(second, first)::matches, List.of());
    }

    // No any type changes for `subtraction` operation from planner perspective.
    @ParameterizedTest
    @MethodSource("allNumericPairs")
    public void subtractionOp(NumericPair pair) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.second());

        Matcher<RexNode> first = ofTypeWithoutCast(pair.first());
        Matcher<RexNode> second = ofTypeWithoutCast(pair.second());

        assertPlan("SELECT c1 - c2 FROM t", schema, operandMatcher(first, second)::matches, List.of());
        assertPlan("SELECT c2 - c1 FROM t", schema, operandMatcher(second, first)::matches, List.of());
    }

    // No any type changes for `division` operation from planner perspective.
    @ParameterizedTest
    @MethodSource("allNumericPairs")
    public void divisionOp(NumericPair pair) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.second());

        Matcher<RexNode> first = ofTypeWithoutCast(pair.first());
        Matcher<RexNode> second = ofTypeWithoutCast(pair.second());

        assertPlan("SELECT c1 / c2 FROM t", schema, operandMatcher(first, second)::matches, List.of());
        assertPlan("SELECT c2 / c1 FROM t", schema, operandMatcher(second, first)::matches, List.of());
    }

    // No any type changes for `multiplication` operation from planner perspective.
    @ParameterizedTest
    @MethodSource("allNumericPairs")
    public void multiplicationOp(NumericPair pair) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.second());

        Matcher<RexNode> first = ofTypeWithoutCast(pair.first());
        Matcher<RexNode> second = ofTypeWithoutCast(pair.second());

        assertPlan("SELECT c1 * c2 FROM t", schema, operandMatcher(first, second)::matches, List.of());
        assertPlan("SELECT c2 * c1 FROM t", schema, operandMatcher(second, first)::matches, List.of());
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
            Matcher<RexNode> secondOperandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        assertPlan("SELECT c1 % c2 FROM t", schema, operandMatcher(firstOperandMatcher, secondOperandMatcher)::matches, List.of());
        assertPlan("SELECT c2 % c1 FROM t", schema, operandMatcher(secondOperandMatcher, firstOperandMatcher)::matches, List.of());
    }

    /**
     * This test ensures that {@link #moduloArgs()} doesn't miss any type pair from {@link NumericPair}.
     */
    @Test
    void moduloArgsIncludesAllTypePairs() {
        checkIncludesAllTypePairs(moduloArgs());
    }

    private static Stream<Arguments> moduloArgs() {
        return Stream.of(
                forTypePair(NumericPair.TINYINT_TINYINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_SMALLINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_INT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_BIGINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_NUMBER_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_NUMBER_2)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_NUMBER_5)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_DECIMAL_1_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7)),

                forTypePair(NumericPair.TINYINT_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15)),


                forTypePair(NumericPair.SMALLINT_SMALLINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_INT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_BIGINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_NUMBER_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_NUMBER_2)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_NUMBER_5)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_DECIMAL_1_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7)),

                forTypePair(NumericPair.SMALLINT_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15)),


                forTypePair(NumericPair.INT_INT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_BIGINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_NUMBER_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_NUMBER_2)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_NUMBER_5)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_DECIMAL_1_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7)),

                forTypePair(NumericPair.INT_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15)),


                forTypePair(NumericPair.BIGINT_BIGINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_NUMBER_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_NUMBER_2)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_NUMBER_5)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_DECIMAL_1_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7)),

                forTypePair(NumericPair.BIGINT_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15)),


                forTypePair(NumericPair.NUMBER_1_NUMBER_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_1_NUMBER_2)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_1_NUMBER_5)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_1_DECIMAL_1_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_1_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_1_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_1_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_1_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_1_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_1_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_1_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_1_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_1_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7)),

                forTypePair(NumericPair.NUMBER_1_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15)),


                forTypePair(NumericPair.NUMBER_2_NUMBER_2)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_2_NUMBER_5)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_2_DECIMAL_1_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_2_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_2_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_2_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_2_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_2_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_2_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_2_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_2_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_2_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7)),

                forTypePair(NumericPair.NUMBER_2_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15)),


                forTypePair(NumericPair.NUMBER_5_NUMBER_5)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_5_DECIMAL_1_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_5_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_5_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_5_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_5_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_5_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_5_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_5_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_5_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.NUMBER_5_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7)),

                forTypePair(NumericPair.NUMBER_5_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15)),


                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_1_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7)),

                forTypePair(NumericPair.DECIMAL_1_0_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15)),


                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_1_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7)),

                forTypePair(NumericPair.DECIMAL_2_1_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15)),


                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_4_3_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7)),

                forTypePair(NumericPair.DECIMAL_4_3_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15)),


                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_0_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7)),

                forTypePair(NumericPair.DECIMAL_2_0_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15)),


                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_3_1_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7)),

                forTypePair(NumericPair.DECIMAL_3_1_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15)),


                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_5_3_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7)),

                forTypePair(NumericPair.DECIMAL_5_3_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15)),


                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_5_0_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7)),

                forTypePair(NumericPair.DECIMAL_5_0_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15)),


                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_6_1_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7)),

                forTypePair(NumericPair.DECIMAL_6_1_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15)),


                forTypePair(NumericPair.DECIMAL_8_3_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_8_3_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7)),

                forTypePair(NumericPair.DECIMAL_8_3_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15)),


                forTypePair(NumericPair.REAL_REAL)
                        .firstOpMatches(castTo(Types.DECIMAL_14_7))
                        .secondOpMatches(castTo((Types.DECIMAL_14_7))),

                forTypePair(NumericPair.REAL_DOUBLE)
                        .firstOpMatches(castTo(Types.DECIMAL_14_7))
                        .secondOpMatches(castTo(Types.DECIMAL_30_15)),


                forTypePair(NumericPair.DOUBLE_DOUBLE)
                        .firstOpMatches(castTo(Types.DECIMAL_30_15))
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
        );
    }
}
