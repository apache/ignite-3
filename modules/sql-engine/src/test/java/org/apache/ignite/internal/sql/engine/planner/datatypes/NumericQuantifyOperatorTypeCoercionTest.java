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
import org.apache.ignite.internal.type.NativeTypes;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of tests to verify behavior of type coercion for quantify operators (ALL, SOME), when operands belongs to the NUMERIC type family.
 *
 * <p>This tests aim to help to understand in which cases implicit cast will be added to which operand.
 */
public class NumericQuantifyOperatorTypeCoercionTest extends BaseTypeCoercionTest {
    private static Stream<Arguments> args() {
        return Stream.of(
                forTypePair(NumericPair.TINYINT_TINYINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_SMALLINT)
                        .firstOpMatches(ofType(NativeTypes.INT16))
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_INT)
                        .firstOpMatches(ofType(NativeTypes.INT32))
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_BIGINT)
                        .firstOpMatches(ofType(NativeTypes.INT64))
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_DECIMAL_1_0)
                        .firstOpMatches(ofType(Types.DECIMAL_3_0))
                        .secondOpMatches(ofType(Types.DECIMAL_3_0)),

                forTypePair(NumericPair.TINYINT_DECIMAL_2_1)
                        .firstOpMatches(ofType(Types.DECIMAL_4_1))
                        .secondOpMatches(ofType(Types.DECIMAL_4_1)),

                forTypePair(NumericPair.TINYINT_DECIMAL_4_3)
                        .firstOpMatches(ofType(Types.DECIMAL_6_3))
                        .secondOpMatches(ofType(Types.DECIMAL_6_3)),

                forTypePair(NumericPair.TINYINT_DECIMAL_2_0)
                        .firstOpMatches(ofType(Types.DECIMAL_3_0))
                        .secondOpMatches(ofType(Types.DECIMAL_3_0)),

                forTypePair(NumericPair.TINYINT_DECIMAL_3_1)
                        .firstOpMatches(ofType(Types.DECIMAL_4_1))
                        .secondOpMatches(ofType(Types.DECIMAL_4_1)),

                forTypePair(NumericPair.TINYINT_DECIMAL_5_3)
                        .firstOpMatches(ofType(Types.DECIMAL_6_3))
                        .secondOpMatches(ofType(Types.DECIMAL_6_3)),

                forTypePair(NumericPair.TINYINT_DECIMAL_5_0)
                        .firstOpMatches(ofType(Types.DECIMAL_5_0))
                        .secondOpMatches(ofType(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.TINYINT_DECIMAL_6_1)
                        .firstOpMatches(ofType(Types.DECIMAL_6_1))
                        .secondOpMatches(ofType(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.TINYINT_DECIMAL_8_3)
                        .firstOpMatches(ofType(Types.DECIMAL_8_3))
                        .secondOpMatches(ofType(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.TINYINT_REAL)
                        .firstOpMatches(ofType(NativeTypes.FLOAT))
                        .secondOpMatches(ofType(NativeTypes.FLOAT)),

                forTypePair(NumericPair.TINYINT_DOUBLE)
                        .firstOpMatches(ofType(NativeTypes.DOUBLE))
                        .secondOpMatches(ofType(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.SMALLINT_SMALLINT)
                        .firstOpMatches(ofType(NativeTypes.INT16))
                        .secondOpMatches(ofType(NativeTypes.INT16)),

                forTypePair(NumericPair.SMALLINT_INT)
                        .firstOpMatches(ofType(NativeTypes.INT32))
                        .secondOpMatches(ofType(NativeTypes.INT32)),

                forTypePair(NumericPair.SMALLINT_BIGINT)
                        .firstOpMatches(ofType(NativeTypes.INT64))
                        .secondOpMatches(ofType(NativeTypes.INT64)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_1_0)
                        .firstOpMatches(ofType(Types.DECIMAL_5_0))
                        .secondOpMatches(ofType(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_2_1)
                        .firstOpMatches(ofType(Types.DECIMAL_6_1))
                        .secondOpMatches(ofType(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_4_3)
                        .firstOpMatches(ofType(Types.DECIMAL_8_3))
                        .secondOpMatches(ofType(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_2_0)
                        .firstOpMatches(ofType(Types.DECIMAL_5_0))
                        .secondOpMatches(ofType(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_3_1)
                        .firstOpMatches(ofType(Types.DECIMAL_6_1))
                        .secondOpMatches(ofType(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_5_3)
                        .firstOpMatches(ofType(Types.DECIMAL_8_3))
                        .secondOpMatches(ofType(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_5_0)
                        .firstOpMatches(ofType(Types.DECIMAL_5_0))
                        .secondOpMatches(ofType(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_6_1)
                        .firstOpMatches(ofType(Types.DECIMAL_6_1))
                        .secondOpMatches(ofType(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_8_3)
                        .firstOpMatches(ofType(Types.DECIMAL_8_3))
                        .secondOpMatches(ofType(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.SMALLINT_REAL)
                        .firstOpMatches(ofType(NativeTypes.FLOAT))
                        .secondOpMatches(ofType(NativeTypes.FLOAT)),

                forTypePair(NumericPair.SMALLINT_DOUBLE)
                        .firstOpMatches(ofType(NativeTypes.DOUBLE))
                        .secondOpMatches(ofType(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.INT_INT)
                        .firstOpMatches(ofType(NativeTypes.INT32))
                        .secondOpMatches(ofType(NativeTypes.INT32)),

                forTypePair(NumericPair.INT_BIGINT)
                        .firstOpMatches(ofType(NativeTypes.INT64))
                        .secondOpMatches(ofType(NativeTypes.INT64)),

                forTypePair(NumericPair.INT_DECIMAL_1_0)
                        .firstOpMatches(ofType(Types.DECIMAL_10_0))
                        .secondOpMatches(ofType(Types.DECIMAL_10_0)),

                forTypePair(NumericPair.INT_DECIMAL_2_1)
                        .firstOpMatches(ofType(Types.DECIMAL_11_1))
                        .secondOpMatches(ofType(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.INT_DECIMAL_4_3)
                        .firstOpMatches(ofType(Types.DECIMAL_13_3))
                        .secondOpMatches(ofType(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.INT_DECIMAL_2_0)
                        .firstOpMatches(ofType(Types.DECIMAL_10_0))
                        .secondOpMatches(ofType(Types.DECIMAL_10_0)),

                forTypePair(NumericPair.INT_DECIMAL_3_1)
                        .firstOpMatches(ofType(Types.DECIMAL_11_1))
                        .secondOpMatches(ofType(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.INT_DECIMAL_5_3)
                        .firstOpMatches(ofType(Types.DECIMAL_13_3))
                        .secondOpMatches(ofType(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.INT_DECIMAL_5_0)
                        .firstOpMatches(ofType(Types.DECIMAL_10_0))
                        .secondOpMatches(ofType(Types.DECIMAL_10_0)),

                forTypePair(NumericPair.INT_DECIMAL_6_1)
                        .firstOpMatches(ofType(Types.DECIMAL_11_1))
                        .secondOpMatches(ofType(Types.DECIMAL_11_1)),

                forTypePair(NumericPair.INT_DECIMAL_8_3)
                        .firstOpMatches(ofType(Types.DECIMAL_13_3))
                        .secondOpMatches(ofType(Types.DECIMAL_13_3)),

                forTypePair(NumericPair.INT_REAL)
                        .firstOpMatches(ofType(NativeTypes.FLOAT))
                        .secondOpMatches(ofType(NativeTypes.FLOAT)),

                forTypePair(NumericPair.INT_DOUBLE)
                        .firstOpMatches(ofType(NativeTypes.DOUBLE))
                        .secondOpMatches(ofType(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.BIGINT_BIGINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_DECIMAL_1_0)
                        .firstOpMatches(ofType(Types.DECIMAL_19_0))
                        .secondOpMatches(ofType(Types.DECIMAL_19_0)),

                forTypePair(NumericPair.BIGINT_DECIMAL_2_1)
                        .firstOpMatches(ofType(Types.DECIMAL_20_1))
                        .secondOpMatches(ofType(Types.DECIMAL_20_1)),

                forTypePair(NumericPair.BIGINT_DECIMAL_4_3)
                        .firstOpMatches(ofType(Types.DECIMAL_22_3))
                        .secondOpMatches(ofType(Types.DECIMAL_22_3)),

                forTypePair(NumericPair.BIGINT_DECIMAL_2_0)
                        .firstOpMatches(ofType(Types.DECIMAL_19_0))
                        .secondOpMatches(ofType(Types.DECIMAL_19_0)),

                forTypePair(NumericPair.BIGINT_DECIMAL_3_1)
                        .firstOpMatches(ofType(Types.DECIMAL_20_1))
                        .secondOpMatches(ofType(Types.DECIMAL_20_1)),

                forTypePair(NumericPair.BIGINT_DECIMAL_5_3)
                        .firstOpMatches(ofType(Types.DECIMAL_22_3))
                        .secondOpMatches(ofType(Types.DECIMAL_22_3)),

                forTypePair(NumericPair.BIGINT_DECIMAL_5_0)
                        .firstOpMatches(ofType(Types.DECIMAL_19_0))
                        .secondOpMatches(ofType(Types.DECIMAL_19_0)),

                forTypePair(NumericPair.BIGINT_DECIMAL_6_1)
                        .firstOpMatches(ofType(Types.DECIMAL_20_1))
                        .secondOpMatches(ofType(Types.DECIMAL_20_1)),

                forTypePair(NumericPair.BIGINT_DECIMAL_8_3)
                        .firstOpMatches(ofType(Types.DECIMAL_22_3))
                        .secondOpMatches(ofType(Types.DECIMAL_22_3)),

                forTypePair(NumericPair.BIGINT_REAL)
                        .firstOpMatches(ofType(NativeTypes.FLOAT))
                        .secondOpMatches(ofType(NativeTypes.FLOAT)),

                forTypePair(NumericPair.BIGINT_DOUBLE)
                        .firstOpMatches(ofType(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_1_0)
                        .firstOpMatches(ofType(Types.DECIMAL_1_0))
                        .secondOpMatches(ofType(Types.DECIMAL_1_0)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_1)
                        .firstOpMatches(ofType(Types.DECIMAL_2_1))
                        .secondOpMatches(ofType(Types.DECIMAL_2_1)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_4_3)
                        .firstOpMatches(ofType(Types.DECIMAL_4_3))
                        .secondOpMatches(ofType(Types.DECIMAL_4_3)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_0)
                        .firstOpMatches(ofType(Types.DECIMAL_2_0))
                        .secondOpMatches(ofType(Types.DECIMAL_2_0)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_3_1)
                        .firstOpMatches(ofType(Types.DECIMAL_3_1))
                        .secondOpMatches(ofType(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_3)
                        .firstOpMatches(ofType(Types.DECIMAL_5_3))
                        .secondOpMatches(ofType(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_0)
                        .firstOpMatches(ofType(Types.DECIMAL_5_0))
                        .secondOpMatches(ofType(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_6_1)
                        .firstOpMatches(ofType(Types.DECIMAL_6_1))
                        .secondOpMatches(ofType(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_8_3)
                        .firstOpMatches(ofType(Types.DECIMAL_8_3))
                        .secondOpMatches(ofType(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_1_0_REAL)
                        .firstOpMatches(ofType(NativeTypes.FLOAT))
                        .secondOpMatches(ofType(NativeTypes.FLOAT)),

                forTypePair(NumericPair.DECIMAL_1_0_DOUBLE)
                        .firstOpMatches(ofType(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_1)
                        .firstOpMatches(ofType(Types.DECIMAL_2_1))
                        .secondOpMatches(ofType(Types.DECIMAL_2_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_4_3)
                        .firstOpMatches(ofType(Types.DECIMAL_4_3))
                        .secondOpMatches(ofType(Types.DECIMAL_4_3)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_0)
                        .firstOpMatches(ofType(Types.DECIMAL_3_1))
                        .secondOpMatches(ofType(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_3_1)
                        .firstOpMatches(ofType(Types.DECIMAL_3_1))
                        .secondOpMatches(ofType(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_3)
                        .firstOpMatches(ofType(Types.DECIMAL_5_3))
                        .secondOpMatches(ofType(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_0)
                        .firstOpMatches(ofType(Types.DECIMAL_6_1))
                        .secondOpMatches(ofType(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_6_1)
                        .firstOpMatches(ofType(Types.DECIMAL_6_1))
                        .secondOpMatches(ofType(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_8_3)
                        .firstOpMatches(ofType(Types.DECIMAL_8_3))
                        .secondOpMatches(ofType(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_2_1_REAL)
                        .firstOpMatches(ofType(NativeTypes.FLOAT))
                        .secondOpMatches(ofType(NativeTypes.FLOAT)),

                forTypePair(NumericPair.DECIMAL_2_1_DOUBLE)
                        .firstOpMatches(ofType(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_4_3)
                        .firstOpMatches(ofType(Types.DECIMAL_4_3))
                        .secondOpMatches(ofType(Types.DECIMAL_4_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_2_0)
                        .firstOpMatches(ofType(Types.DECIMAL_5_3))
                        .secondOpMatches(ofType(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_3_1)
                        .firstOpMatches(ofType(Types.DECIMAL_5_3))
                        .secondOpMatches(ofType(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_3)
                        .firstOpMatches(ofType(Types.DECIMAL_5_3))
                        .secondOpMatches(ofType(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_0)
                        .firstOpMatches(ofType(Types.DECIMAL_8_3))
                        .secondOpMatches(ofType(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_6_1)
                        .firstOpMatches(ofType(Types.DECIMAL_8_3))
                        .secondOpMatches(ofType(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_8_3)
                        .firstOpMatches(ofType(Types.DECIMAL_8_3))
                        .secondOpMatches(ofType(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_4_3_REAL)
                        .firstOpMatches(ofType(NativeTypes.FLOAT))
                        .secondOpMatches(ofType(NativeTypes.FLOAT)),

                forTypePair(NumericPair.DECIMAL_4_3_DOUBLE)
                        .firstOpMatches(ofType(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_2_0)
                        .firstOpMatches(ofType(Types.DECIMAL_2_0))
                        .secondOpMatches(ofType(Types.DECIMAL_2_0)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_3_1)
                        .firstOpMatches(ofType(Types.DECIMAL_3_1))
                        .secondOpMatches(ofType(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_3)
                        .firstOpMatches(ofType(Types.DECIMAL_5_3))
                        .secondOpMatches(ofType(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_0)
                        .firstOpMatches(ofType(Types.DECIMAL_5_0))
                        .secondOpMatches(ofType(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_6_1)
                        .firstOpMatches(ofType(Types.DECIMAL_6_1))
                        .secondOpMatches(ofType(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_8_3)
                        .firstOpMatches(ofType(Types.DECIMAL_8_3))
                        .secondOpMatches(ofType(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_2_0_REAL)
                        .firstOpMatches(ofType(NativeTypes.FLOAT))
                        .secondOpMatches(ofType(NativeTypes.FLOAT)),

                forTypePair(NumericPair.DECIMAL_2_0_DOUBLE)
                        .firstOpMatches(ofType(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_3_1)
                        .firstOpMatches(ofType(Types.DECIMAL_3_1))
                        .secondOpMatches(ofType(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_3)
                        .firstOpMatches(ofType(Types.DECIMAL_5_3))
                        .secondOpMatches(ofType(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_0)
                        .firstOpMatches(ofType(Types.DECIMAL_6_1))
                        .secondOpMatches(ofType(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_6_1)
                        .firstOpMatches(ofType(Types.DECIMAL_6_1))
                        .secondOpMatches(ofType(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_8_3)
                        .firstOpMatches(ofType(Types.DECIMAL_8_3))
                        .secondOpMatches(ofType(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_3_1_REAL)
                        .firstOpMatches(ofType(NativeTypes.FLOAT))
                        .secondOpMatches(ofType(NativeTypes.FLOAT)),

                forTypePair(NumericPair.DECIMAL_3_1_DOUBLE)
                        .firstOpMatches(ofType(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_3)
                        .firstOpMatches(ofType(Types.DECIMAL_5_3))
                        .secondOpMatches(ofType(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_0)
                        .firstOpMatches(ofType(Types.DECIMAL_8_3))
                        .secondOpMatches(ofType(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_6_1)
                        .firstOpMatches(ofType(Types.DECIMAL_8_3))
                        .secondOpMatches(ofType(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_8_3)
                        .firstOpMatches(ofType(Types.DECIMAL_8_3))
                        .secondOpMatches(ofType(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_5_3_REAL)
                        .firstOpMatches(ofType(NativeTypes.FLOAT))
                        .secondOpMatches(ofType(NativeTypes.FLOAT)),

                forTypePair(NumericPair.DECIMAL_5_3_DOUBLE)
                        .firstOpMatches(ofType(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_5_0)
                        .firstOpMatches(ofType(Types.DECIMAL_5_0))
                        .secondOpMatches(ofType(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_6_1)
                        .firstOpMatches(ofType(Types.DECIMAL_6_1))
                        .secondOpMatches(ofType(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_8_3)
                        .firstOpMatches(ofType(Types.DECIMAL_8_3))
                        .secondOpMatches(ofType(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_5_0_REAL)
                        .firstOpMatches(ofType(NativeTypes.FLOAT))
                        .secondOpMatches(ofType(NativeTypes.FLOAT)),

                forTypePair(NumericPair.DECIMAL_5_0_DOUBLE)
                        .firstOpMatches(ofType(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_6_1)
                        .firstOpMatches(ofType(Types.DECIMAL_6_1))
                        .secondOpMatches(ofType(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_8_3)
                        .firstOpMatches(ofType(Types.DECIMAL_8_3))
                        .secondOpMatches(ofType(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_6_1_REAL)
                        .firstOpMatches(ofType(NativeTypes.FLOAT))
                        .secondOpMatches(ofType(NativeTypes.FLOAT)),

                forTypePair(NumericPair.DECIMAL_6_1_DOUBLE)
                        .firstOpMatches(ofType(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.DECIMAL_8_3_DECIMAL_8_3)
                        .firstOpMatches(ofType(Types.DECIMAL_8_3))
                        .secondOpMatches(ofType(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_8_3_REAL)
                        .firstOpMatches(ofType(NativeTypes.FLOAT))
                        .secondOpMatches(ofType(NativeTypes.FLOAT)),

                forTypePair(NumericPair.DECIMAL_8_3_DOUBLE)
                        .firstOpMatches(ofType(NativeTypes.DOUBLE))
                        .secondOpBeSame(),

                forTypePair(NumericPair.REAL_REAL)
                        .firstOpMatches(ofType(NativeTypes.FLOAT))
                        .secondOpMatches(ofType(NativeTypes.FLOAT)),

                forTypePair(NumericPair.REAL_DOUBLE)
                        .firstOpMatches(ofType(NativeTypes.DOUBLE))
                        .secondOpMatches(ofType(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DOUBLE_DOUBLE)
                        .firstOpMatches(ofType(NativeTypes.DOUBLE))
                        .secondOpMatches(ofType(NativeTypes.DOUBLE))
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
        checkIncludesAllNumericTypePairs(args());
    }
}
