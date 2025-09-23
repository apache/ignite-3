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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.prepare.IgniteSqlValidator.DECIMAL_DYNAMIC_PARAM_PRECISION;
import static org.apache.ignite.internal.sql.engine.prepare.IgniteSqlValidator.DECIMAL_DYNAMIC_PARAM_SCALE;

import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.NumericPair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of tests to verify behavior of type coercion for CASE operator, when operands belongs to the NUMERIC type family.
 *
 * <p>This tests aim to help to understand in which cases implicit cast will be added to which operand.
 */
public class NumericCaseTypeCoercionTest extends BaseTypeCoercionTest {
    private static final IgniteSchema SCHEMA = createSchemaWithTwoColumnTable(NativeTypes.STRING, NativeTypes.STRING);

    private static final NativeType DECIMAL_DYN_PARAM_DEFAULT = NativeTypes.decimalOf(
            DECIMAL_DYNAMIC_PARAM_PRECISION, DECIMAL_DYNAMIC_PARAM_SCALE
    );

    /** CASE operands from columns. */
    @ParameterizedTest
    @MethodSource("caseArgs")
    public void numericCoercion(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        assertPlan("SELECT CASE WHEN RAND_UUID() != RAND_UUID() THEN c1 ELSE c2 END FROM t", schema,
                operandCaseMatcher(firstOperandMatcher, secondOperandMatcher)::matches, List.of());
    }

    /** CASE operands from dynamic params. */
    @ParameterizedTest
    @MethodSource("dynamicLiteralArgs")
    public void numericWithDynamicParamsCoercion(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher
    ) throws Exception {
        List<Object> params = List.of(
                SqlTestUtils.generateValueByType(typePair.first()),
                SqlTestUtils.generateValueByType(typePair.second())
        );

        assertPlan("SELECT CASE WHEN RAND_UUID() != RAND_UUID() THEN ? ELSE ? END FROM t", SCHEMA,
                operandCaseMatcher(firstOperandMatcher, secondOperandMatcher)::matches, params);
    }

    /** CASE operands from literals. */
    @ParameterizedTest
    @MethodSource("literalArgs")
    public void numericWithLiteralsCoercion(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher
    ) throws Exception {
        List<Object> params = List.of(
                generateLiteralWithNoRepetition(typePair.first()), generateLiteralWithNoRepetition(typePair.second())
        );

        assertPlan(format("SELECT CASE WHEN RAND_UUID() != RAND_UUID() THEN {} ELSE {} END FROM t", params.get(0), params.get(1)),
                SCHEMA, operandCaseMatcher(firstOperandMatcher, secondOperandMatcher)::matches, List.of());
    }

    private static Stream<Arguments> literalArgs() {
        return Stream.of(
                forTypePair(NumericPair.TINYINT_TINYINT)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT32))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.TINYINT_SMALLINT)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT32))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.TINYINT_INT)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT32))
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_BIGINT)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT64))
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_DECIMAL_1_0)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT32))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.TINYINT_DECIMAL_2_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.TINYINT_DECIMAL_4_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.TINYINT_DECIMAL_2_0)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT32))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.TINYINT_DECIMAL_3_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.TINYINT_DECIMAL_5_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.TINYINT_DECIMAL_5_0)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT32))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.TINYINT_DECIMAL_6_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.TINYINT_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.TINYINT_REAL)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.TINYINT_DOUBLE)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.SMALLINT_SMALLINT)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT32))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.SMALLINT_INT)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT32))
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_BIGINT)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT64))
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_DECIMAL_1_0)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT32))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_2_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_4_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_2_0)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT32))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_3_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_5_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_5_0)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT32))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_6_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.SMALLINT_REAL)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.SMALLINT_DOUBLE)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_INT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_BIGINT)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT64))
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_DECIMAL_1_0)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT32))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.INT_DECIMAL_2_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.INT_DECIMAL_4_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.INT_DECIMAL_2_0)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT32))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.INT_DECIMAL_3_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.INT_DECIMAL_5_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.INT_DECIMAL_5_0)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT32))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.INT_DECIMAL_6_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.INT_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.INT_REAL)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.INT_DOUBLE)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_BIGINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_DECIMAL_1_0)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT64))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT64)),

                forTypePair(NumericPair.BIGINT_DECIMAL_2_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_20_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_20_1)),

                forTypePair(NumericPair.BIGINT_DECIMAL_4_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_22_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_22_3)),

                forTypePair(NumericPair.BIGINT_DECIMAL_2_0)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT64))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT64)),

                forTypePair(NumericPair.BIGINT_DECIMAL_3_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_20_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_20_1)),

                forTypePair(NumericPair.BIGINT_DECIMAL_5_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_22_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_22_3)),

                forTypePair(NumericPair.BIGINT_DECIMAL_5_0)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT64))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT64)),

                forTypePair(NumericPair.BIGINT_DECIMAL_6_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_20_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_20_1)),

                forTypePair(NumericPair.BIGINT_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_22_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_22_3)),

                forTypePair(NumericPair.BIGINT_REAL)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.BIGINT_DOUBLE)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_1_0)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT32))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_4_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_0)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT32))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_3_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_0)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT32))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_6_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.DECIMAL_1_0_REAL)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_1_0_DOUBLE)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_2_0)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT32))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_3_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_0)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT32))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_6_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.DECIMAL_2_0_REAL)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_2_0_DOUBLE)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_2_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_2_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_4_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_4_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_4_3)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_3_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_3_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_6_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_6_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_2_1_REAL)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_2_1_DOUBLE)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_4_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_4_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_4_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_2_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_3_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_6_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_4_3_REAL)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_4_3_DOUBLE)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_3_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_3_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_6_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_6_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_3_1_REAL)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_3_1_DOUBLE)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_6_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_5_3_REAL)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_5_3_DOUBLE)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_5_0)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT32))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_6_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.DECIMAL_5_0_REAL)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_5_0_DOUBLE)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_6_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_6_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_6_1_REAL)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_6_1_DOUBLE)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_8_3_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_8_3_REAL)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_8_3_DOUBLE)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.REAL_REAL)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.REAL_DOUBLE)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DOUBLE_DOUBLE)
                        .firstOpBeSame()
                        .secondOpBeSame()
        );
    }

    /**
     * This test ensures that {@link #literalArgs()} doesn't miss any type pair from {@link NumericPair}.
     */
    @Test
    void litArgsIncludesAllTypePairs() {
        checkIncludesAllNumericTypePairs(literalArgs());
    }

    private static Stream<Arguments> dynamicLiteralArgs() {
        return Stream.of(
                forTypePair(NumericPair.TINYINT_TINYINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_SMALLINT)
                        .firstOpMatches(castTo(NativeTypes.INT16))
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_INT)
                        .firstOpMatches(castTo(NativeTypes.INT32))
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_BIGINT)
                        .firstOpMatches(castTo(NativeTypes.INT64))
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_DECIMAL_1_0)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.TINYINT_DECIMAL_2_1)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.TINYINT_DECIMAL_4_3)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.TINYINT_DECIMAL_2_0)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.TINYINT_DECIMAL_3_1)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.TINYINT_DECIMAL_5_3)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.TINYINT_DECIMAL_5_0)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.TINYINT_DECIMAL_6_1)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.TINYINT_DECIMAL_8_3)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.TINYINT_REAL)
                        .firstOpMatches(castTo(NativeTypes.FLOAT))
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_SMALLINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_INT)
                        .firstOpMatches(castTo(NativeTypes.INT32))
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_BIGINT)
                        .firstOpMatches(castTo(NativeTypes.INT64))
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_DECIMAL_1_0)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_2_1)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_4_3)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_2_0)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_3_1)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_5_3)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_5_0)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_6_1)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_8_3)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.SMALLINT_REAL)
                        .firstOpMatches(castTo(NativeTypes.FLOAT))
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_INT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_BIGINT)
                        .firstOpMatches(castTo(NativeTypes.INT64))
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_DECIMAL_1_0)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.INT_DECIMAL_2_1)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.INT_DECIMAL_4_3)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.INT_DECIMAL_2_0)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.INT_DECIMAL_3_1)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.INT_DECIMAL_5_3)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.INT_DECIMAL_5_0)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.INT_DECIMAL_6_1)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.INT_DECIMAL_8_3)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.INT_REAL)
                        .firstOpMatches(castTo(NativeTypes.FLOAT))
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_BIGINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_DECIMAL_1_0)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.BIGINT_DECIMAL_2_1)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.BIGINT_DECIMAL_4_3)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.BIGINT_DECIMAL_2_0)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.BIGINT_DECIMAL_3_1)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.BIGINT_DECIMAL_5_3)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.BIGINT_DECIMAL_5_0)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.BIGINT_DECIMAL_6_1)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.BIGINT_DECIMAL_8_3)
                        .firstOpMatches(castTo(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.BIGINT_REAL)
                        .firstOpMatches(castTo(NativeTypes.FLOAT))
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_1_0)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_1)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_4_3)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_0)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_3_1)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_3)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_0)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_6_1)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_1_0_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_1_0_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_2_0)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_3_1)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_3)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_0)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_6_1)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_2_0_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_2_0_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_1)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_4_3)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_0)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_3_1)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_3)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_0)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_6_1)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_2_1_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_2_1_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_4_3)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_2_0)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_3_1)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_3)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_0)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_6_1)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_4_3_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_4_3_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_3_1)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_3)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_0)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_6_1)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_3_1_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_3_1_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_3)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_0)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_6_1)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_5_3_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_5_3_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_5_0)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_6_1)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_5_0_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_5_0_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_6_1)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_6_1_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_6_1_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_8_3_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)),

                forTypePair(NumericPair.DECIMAL_8_3_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_8_3_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.REAL_REAL)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.REAL_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DOUBLE_DOUBLE)
                        .firstOpBeSame()
                        .secondOpBeSame()
        );
    }

    /**
     * This test ensures that {@link #dynamicLiteralArgs()} doesn't miss any type pair from {@link NumericPair}.
     */
    @Test
    void dynArgsIncludesAllTypePairs() {
        checkIncludesAllNumericTypePairs(dynamicLiteralArgs());
    }

    private static Stream<Arguments> caseArgs() {
        return Stream.of(
                forTypePair(NumericPair.TINYINT_TINYINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_SMALLINT)
                        .firstOpMatches(castTo(NativeTypes.INT16))
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_INT)
                        .firstOpMatches(castTo(NativeTypes.INT32))
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_BIGINT)
                        .firstOpMatches(castTo(NativeTypes.INT64))
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_DECIMAL_1_0)
                        .firstOpMatches(castTo(Types.DECIMAL_3_0))
                        .secondOpMatches(castTo(Types.DECIMAL_3_0)),

                forTypePair(NumericPair.TINYINT_DECIMAL_2_1)
                        .firstOpMatches(castTo(Types.DECIMAL_4_1))
                        .secondOpMatches(castTo(Types.DECIMAL_4_1)),

                forTypePair(NumericPair.TINYINT_DECIMAL_4_3)
                        .firstOpMatches(castTo(Types.DECIMAL_6_3))
                        .secondOpMatches(castTo(Types.DECIMAL_6_3)),

                forTypePair(NumericPair.TINYINT_DECIMAL_2_0)
                        .firstOpMatches(castTo(Types.DECIMAL_3_0))
                        .secondOpMatches(castTo(Types.DECIMAL_3_0)),

                forTypePair(NumericPair.TINYINT_DECIMAL_3_1)
                        .firstOpMatches(castTo(Types.DECIMAL_4_1))
                        .secondOpMatches(castTo(Types.DECIMAL_4_1)),

                forTypePair(NumericPair.TINYINT_DECIMAL_5_3)
                        .firstOpMatches(castTo(Types.DECIMAL_6_3))
                        .secondOpMatches(castTo(Types.DECIMAL_6_3)),

                forTypePair(NumericPair.TINYINT_DECIMAL_5_0)
                        .firstOpMatches(castTo(Types.DECIMAL_5_0))
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_REAL)
                        .firstOpMatches(castTo(NativeTypes.FLOAT))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.FLOAT)),

                forTypePair(NumericPair.TINYINT_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_SMALLINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_INT)
                        .firstOpMatches(castTo(NativeTypes.INT32))
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_BIGINT)
                        .firstOpMatches(castTo(NativeTypes.INT64))
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_DECIMAL_1_0)
                        .firstOpMatches(castTo(Types.DECIMAL_5_0))
                        .secondOpMatches(castTo(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_2_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpMatches(castTo(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_4_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(castTo(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_2_0)
                        .firstOpMatches(castTo(Types.DECIMAL_5_0))
                        .secondOpMatches(castTo(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_3_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpMatches(castTo(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_5_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(castTo(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_5_0)
                        .firstOpMatches(castTo(Types.DECIMAL_5_0))
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_REAL)
                        .firstOpMatches(castTo(NativeTypes.FLOAT))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.FLOAT)),

                forTypePair(NumericPair.SMALLINT_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_INT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_BIGINT)
                        .firstOpMatches(castTo(NativeTypes.INT64))
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_DECIMAL_1_0)
                        .firstOpMatches(castTo(Types.DECIMAL_10_0))
                        .secondOpMatches(castTo(Types.DECIMAL_10_0)),

                forTypePair(NumericPair.INT_DECIMAL_2_1)
                        .firstOpMatches(castTo(Types.DECIMAL_11_1))
                        .secondOpMatches(castTo(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.INT_DECIMAL_4_3)
                        .firstOpMatches(castTo(Types.DECIMAL_13_3))
                        .secondOpMatches(castTo(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.INT_DECIMAL_2_0)
                        .firstOpMatches(castTo(Types.DECIMAL_10_0))
                        .secondOpMatches(castTo(Types.DECIMAL_10_0)),

                forTypePair(NumericPair.INT_DECIMAL_3_1)
                        .firstOpMatches(castTo(Types.DECIMAL_11_1))
                        .secondOpMatches(castTo(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.INT_DECIMAL_5_3)
                        .firstOpMatches(castTo(Types.DECIMAL_13_3))
                        .secondOpMatches(castTo(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.INT_DECIMAL_5_0)
                        .firstOpMatches(castTo(Types.DECIMAL_10_0))
                        .secondOpMatches(castTo(Types.DECIMAL_10_0)),

                forTypePair(NumericPair.INT_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_11_1))
                        .secondOpMatches(castTo(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.INT_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_13_3))
                        .secondOpMatches(castTo(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.INT_REAL)
                        .firstOpMatches(castTo(NativeTypes.FLOAT))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.FLOAT)),

                forTypePair(NumericPair.INT_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_BIGINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_DECIMAL_1_0)
                        .firstOpMatches(castTo(Types.DECIMAL_19_0))
                        .secondOpMatches(castTo(Types.DECIMAL_19_0)),

                forTypePair(NumericPair.BIGINT_DECIMAL_2_1)
                        .firstOpMatches(castTo(Types.DECIMAL_20_1))
                        .secondOpMatches(castTo(Types.DECIMAL_20_1)),

                forTypePair(NumericPair.BIGINT_DECIMAL_4_3)
                        .firstOpMatches(castTo(Types.DECIMAL_22_3))
                        .secondOpMatches(castTo(Types.DECIMAL_22_3)),

                forTypePair(NumericPair.BIGINT_DECIMAL_2_0)
                        .firstOpMatches(castTo(Types.DECIMAL_19_0))
                        .secondOpMatches(castTo(Types.DECIMAL_19_0)),

                forTypePair(NumericPair.BIGINT_DECIMAL_3_1)
                        .firstOpMatches(castTo(Types.DECIMAL_20_1))
                        .secondOpMatches(castTo(Types.DECIMAL_20_1)),

                forTypePair(NumericPair.BIGINT_DECIMAL_5_3)
                        .firstOpMatches(castTo(Types.DECIMAL_22_3))
                        .secondOpMatches(castTo(Types.DECIMAL_22_3)),

                forTypePair(NumericPair.BIGINT_DECIMAL_5_0)
                        .firstOpMatches(castTo(Types.DECIMAL_19_0))
                        .secondOpMatches(castTo(Types.DECIMAL_19_0)),

                forTypePair(NumericPair.BIGINT_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_20_1))
                        .secondOpMatches(castTo(Types.DECIMAL_20_1)),

                forTypePair(NumericPair.BIGINT_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_22_3))
                        .secondOpMatches(castTo(Types.DECIMAL_22_3)),

                forTypePair(NumericPair.BIGINT_REAL)
                        .firstOpMatches(castTo(NativeTypes.FLOAT))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.FLOAT)),

                forTypePair(NumericPair.BIGINT_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_1_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_1)
                        .firstOpMatches(castTo(Types.DECIMAL_2_1))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_4_3)
                        .firstOpMatches(castTo(Types.DECIMAL_4_3))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_0)
                        .firstOpMatches(castTo(Types.DECIMAL_2_0))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_3_1)
                        .firstOpMatches(castTo(Types.DECIMAL_3_1))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_3)
                        .firstOpMatches(castTo(Types.DECIMAL_5_3))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_0)
                        .firstOpMatches(castTo(Types.DECIMAL_5_0))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_1_0_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_4_3)
                        .firstOpMatches(castTo(Types.DECIMAL_4_3))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_0)
                        .firstOpMatches(castTo(Types.DECIMAL_3_1))
                        .secondOpMatches(castTo(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_3_1)
                        .firstOpMatches(castTo(Types.DECIMAL_3_1))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_3)
                        .firstOpMatches(castTo(Types.DECIMAL_5_3))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_0)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpMatches(castTo(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_1_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_2_1_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_2_0)
                        .firstOpMatches(castTo(Types.DECIMAL_5_3))
                        .secondOpMatches(castTo(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_3_1)
                        .firstOpMatches(castTo(Types.DECIMAL_5_3))
                        .secondOpMatches(castTo(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_3)
                        .firstOpMatches(castTo(Types.DECIMAL_5_3))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_0)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(castTo(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(castTo(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_4_3_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_4_3_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_3_1)
                        .firstOpMatches(castTo(Types.DECIMAL_3_1))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_3)
                        .firstOpMatches(castTo(Types.DECIMAL_5_3))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_0)
                        .firstOpMatches(castTo(Types.DECIMAL_5_0))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_0_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_2_0_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_3)
                        .firstOpMatches(castTo(Types.DECIMAL_5_3))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_0)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpMatches(castTo(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_3_1_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_3_1_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_0)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(castTo(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(castTo(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_5_3_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_5_3_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_5_0_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_5_0_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_6_1_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_6_1_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_8_3_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_8_3_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_8_3_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.REAL_REAL)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.REAL_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DOUBLE_DOUBLE)
                        .firstOpBeSame()
                        .secondOpBeSame()
        );
    }

    /**
     * This test ensures that {@link #caseArgs()} doesn't miss any type pair from {@link NumericPair}.
     */
    @Test
    void argsIncludesAllTypePairs() {
        checkIncludesAllNumericTypePairs(caseArgs());
    }
}
